# | Copyright 2016-2017 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, re, time, tempfile
from datetime import datetime, timedelta
from grid_control.backends.aspect_cancel import CancelAndPurgeJobs, CancelJobsWithProcessBlind
from grid_control.backends.aspect_status import CheckInfo, CheckJobsWithProcess
from grid_control.backends.backend_tools import ChunkedExecutor, ProcessCreatorAppendArguments, unpack_wildcard_tar  # pylint:disable=line-too-long
from grid_control.backends.wms import BackendError
from grid_control.backends.wms_grid import GridWMS
from grid_control.job_db import Job
from grid_control.utils import ensure_dir_exists, remove_files, resolve_install_path
from grid_control.utils.activity import Activity
from grid_control.utils.process_base import LocalProcess
from hpfwk import clear_current_exception
from python_compat import imap, irange, md5_hex


class CREAMCancelJobs(CancelJobsWithProcessBlind):
	def __init__(self, config):
		CancelJobsWithProcessBlind.__init__(self, config,
			'glite-ce-job-cancel', ['--noint', '--logfile', '/dev/stderr'])


class CREAMPurgeJobs(CancelJobsWithProcessBlind):
	def __init__(self, config):
		CancelJobsWithProcessBlind.__init__(self, config,
			'glite-ce-job-purge', ['--noint', '--logfile', '/dev/stderr'])


class CREAMCheckJobs(CheckJobsWithProcess):
	def __init__(self, config):
		proc_factory = ProcessCreatorAppendArguments(config,
			'glite-ce-job-status', ['--level', '0', '--logfile', '/dev/stderr'])
		CheckJobsWithProcess.__init__(self, config, proc_factory, status_map={
			Job.ABORTED: ['ABORTED', 'CANCELLED'],
			Job.DONE: ['DONE-FAILED', 'DONE-OK'],
			Job.QUEUED: ['IDLE', 'REGISTERED'],
			Job.RUNNING: ['REALLY-RUNNING', 'RUNNING'],
			Job.UNKNOWN: ['UNKNOWN'],
			Job.WAITING: ['HELD', 'PENDING'],
		})

	def _parse(self, proc):
		job_info = {}
		for line in proc.stdout.iter(self._timeout):
			line = line.lstrip('*').strip()
			try:
				(key, value) = imap(str.strip, line.split('=', 1))
			except Exception:
				clear_current_exception()
				continue
			key = key.lower()
			value = value[1:-1]
			if key == 'jobid':
				yield job_info
				job_info = {CheckInfo.WMSID: value}
			elif key == 'status':
				job_info[CheckInfo.RAW_STATUS] = value
			elif value:
				job_info[key] = value
		yield job_info


class CreamWMS(GridWMS):
	alias_list = ['cream']

	def __init__(self, config, name):
		cancel_executor = CancelAndPurgeJobs(config, CREAMCancelJobs(config), CREAMPurgeJobs(config))
		GridWMS.__init__(self, config, name,
			submit_exec=resolve_install_path('glite-ce-job-submit'),
			output_exec=resolve_install_path('glite-ce-job-output'),
			check_executor=CREAMCheckJobs(config),
			cancel_executor=ChunkedExecutor(config, 'cancel', cancel_executor))

		self._log.info("CreamWMS.__init__")
		self._delegate_exec = resolve_install_path('glite-ce-delegate-proxy')
		self._use_delegate = config.get_bool('try delegate', True, on_change=None)
		self._chunk_size = config.get_int('job chunk size', 10, on_change=None)
		self._submit_args_dict.update({'-r': self._ce, '--config-vo': self._config_fn})
		self._output_regex = r'.*For JobID \[(?P<rawId>\S+)\] output will be stored' + \
			' in the dir (?P<output_dn>.*)$'

		self._end_of_proxy_lifetime = None
		self._set_proxy_lifetime()

	def get_jobs_output_chunk(self, tmp_dn, gc_id_jobnum_list, wms_id_list_done):
		map_gc_id2jobnum = dict(gc_id_jobnum_list)
		jobs = list(self._iter_wms_ids(gc_id_jobnum_list))
		log = tempfile.mktemp('.log')
		proc = LocalProcess(self._output_exec, '--noint', '--logfile', log, '--dir', tmp_dn, *jobs)
		exit_code = proc.status(timeout=20 * len(jobs), terminate=True)

		# yield output dirs
		current_jobnum = None
		for line in imap(str.strip, proc.stdout.iter(timeout=20)):
			match = re.match(self._output_regex, line)
			if match:
				wms_id = match.groupdict()['rawId']
				current_jobnum = map_gc_id2jobnum.get(self._create_gc_id(wms_id))
				wms_id_list_done.append(wms_id)
				yield (current_jobnum, match.groupdict()['output_dn'])
				current_jobnum = None

		if exit_code != 0:
			if 'Keyboard interrupt raised by user' in proc.stdout.read_log():
				remove_files([log, tmp_dn])
				return
			else:
				self._log.log_process(proc)
			self._log.error('Trying to recover from error ...')
			for dn in os.listdir(tmp_dn):
				yield (None, os.path.join(tmp_dn, dn))
		remove_files([log])

	def submit_jobs(self, jobnum_list, task):
		t = self._begin_bulk_submission()
		while not t:
			activity = Activity('waiting before trying to delegate proxy again...')
			time.sleep(900)
			activity.finish()
			activity = Activity('re-attempting to delegate proxy...')
			t = self._begin_bulk_submission()
			activity.finish()
		for result in GridWMS.submit_jobs(self, jobnum_list, task):
			yield result

	def _set_proxy_lifetime(self):
		activity = Activity('Get proxy lifetime...')
		proc = LocalProcess(resolve_install_path('voms-proxy-info'))
		output = proc.get_output(timeout=10, raise_errors=False)
		end_of_proxy = 0
		for l in output.split('\n'):
			if 'timeleft' in l:
				h, m, s = int(l.split(':')[-3]), int(l.split(':')[-2]), int(l.split(':')[-1])
				end_of_proxy = time.time() + h * 60 * 60 + m * 60 + s
				break
		if end_of_proxy == 0:
			self._log.warning('couldnt evaluate end of proxy. Output was:')
			self._log.warning(output)
			time.sleep(300)
			self._set_proxy_lifetime()
		else:
			self._end_of_proxy_lifetime = end_of_proxy
			left_time_str = datetime.fromtimestamp(self._end_of_proxy_lifetime).strftime("%A, %B %d, %Y %I:%M:%S")
			self._log.info('End of current proxy lifetime: %s' % left_time_str)
			activity.finish()
		return 0

	def _begin_bulk_submission(self):
		self._set_proxy_lifetime()
		if self._end_of_proxy_lifetime is None:
			raise Exception("_end_of_proxy_lifetime is not set")

		if self._end_of_proxy_lifetime <= time.time():
			self._log.info("renew proxy is necessary: %s <= %s" % (str(self._end_of_proxy_lifetime), str(time.time())))
			raise Exception("renew proxy is necessary")

		elif '-D' in self._submit_args_dict.keys() and self._submit_args_dict['-D'] is not None:
			try:
				left_time_str = timedelta(seconds=self._end_of_proxy_lifetime - time.time())
			except:
				left_time_str = str(self._end_of_proxy_lifetime - time.time()) + ' sec.'
			self._log.info("Proxy delegation IS NOT ISSUED since expected to be OK. left: %s " % left_time_str)

		else:
			activity = Activity('Delegating proxy for job submission')
			self._submit_args_dict.update({'-D': None})
			t = time.time()
			thehex = md5_hex(str(t))
			self._log.info('Proxy delegation full hex: %s at time %s' % (thehex, str(t)))
			delegate_id = 'GCD' + thehex[:15]
			delegate_arg_list = ['-e', self._ce[:self._ce.rfind("/")]]
			if self._config_fn:
				delegate_arg_list.extend(['--config', self._config_fn])
			proc = LocalProcess(self._delegate_exec, '-d', delegate_id,
				'--logfile', '/dev/stderr', *delegate_arg_list)
			output = proc.get_output(timeout=10, raise_errors=False)
			if ('succesfully delegated to endpoint' in output) and (delegate_id in output):
				self._submit_args_dict.update({'-D': delegate_id})
			activity.finish()

			if proc.status(timeout=0, terminate=True) != 0:
				self._log.log_process(proc)

		return self._submit_args_dict.get('-D') is not None

	def _get_jobs_output(self, gc_id_jobnum_list):
		# Get output of jobs and yield output dirs
		if len(gc_id_jobnum_list) == 0:
			return

		tmp_dn = os.path.join(self._path_output, 'tmp')
		try:
			if len(gc_id_jobnum_list) == 1:
				# For single jobs create single subdir
				tmp_dn = os.path.join(tmp_dn, md5_hex(gc_id_jobnum_list[0][0]))
			ensure_dir_exists(tmp_dn)
		except Exception:
			raise BackendError('Temporary path "%s" could not be created.' % tmp_dn, BackendError)

		map_gc_id2jobnum = dict(gc_id_jobnum_list)
		jobnum_list_todo = list(map_gc_id2jobnum.values())
		wms_id_list_done = []
		activity = Activity('retrieving %d job outputs' % len(gc_id_jobnum_list))
		chunk_pos_iter = irange(0, len(gc_id_jobnum_list), self._chunk_size)
		for ids in imap(lambda x: gc_id_jobnum_list[x:x + self._chunk_size], chunk_pos_iter):
			for (current_jobnum, output_dn) in self.get_jobs_output_chunk(tmp_dn, ids, wms_id_list_done):
				unpack_wildcard_tar(self._log, output_dn)
				jobnum_list_todo.remove(current_jobnum)
				yield (current_jobnum, output_dn)
		activity.finish()

		# return unretrievable jobs
		for jobnum in jobnum_list_todo:
			yield (jobnum, None)
		self._purge_done_jobs(wms_id_list_done)
		remove_files([tmp_dn])

	def _make_jdl(self, jobnum, task):
		return ['[\n'] + GridWMS._make_jdl(self, jobnum, task) + [
			'OutputSandboxBaseDestUri = "gsiftp://localhost";\n]']

	def _purge_done_jobs(self, wms_id_list_done):
		purge_log_fn = tempfile.mktemp('.log')
		purge_proc = LocalProcess(resolve_install_path('glite-ce-job-purge'),
			'--noint', '--logfile', purge_log_fn, str.join(' ', wms_id_list_done))
		exit_code = purge_proc.status(timeout=60)
		if exit_code != 0:
			if self._explain_error(purge_proc, exit_code):
				pass
			else:
				self._log.log_process(purge_proc)
		remove_files([purge_log_fn])
