# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

import os, glob, time, shutil, tempfile
from grid_control import utils
from grid_control.backends.aspect_cancel import CancelAndPurgeJobs, CancelJobs
from grid_control.backends.aspect_status import CheckJobs
from grid_control.backends.broker_base import Broker
from grid_control.backends.wms import BackendError, BasicWMS, WMS
from grid_control.job_db import Job
from grid_control.utils.file_objects import VirtualFile
from grid_control.utils.gc_itertools import lchain
from hpfwk import AbstractError, ExceptionCollector
from python_compat import ifilter, imap, ismap, lfilter, set

class LocalCheckJobs(CheckJobs):
	def __init__(self, config, executor):
		CheckJobs.__init__(self, config)
		self._executor = executor

	def setup(self, log):
		CheckJobs.setup(self, log)
		self._executor.setup(log)

	def execute(self, wmsIDs): # yields list of (wmsID, job_status, job_info)
		checked_ids = set()
		for (wmsID, job_status, job_info) in self._executor.execute(wmsIDs):
			checked_ids.add(wmsID)
			yield (wmsID, job_status, job_info)
		for wmsID in wmsIDs:
			if wmsID not in checked_ids:
				yield (wmsID, Job.DONE, {})


class SandboxHelper(object):
	def __init__(self, config):
		self._cache = []
		self._path = config.getPath('sandbox path', config.getWorkPath('sandbox'), mustExist = False)
		try:
			if not os.path.exists(self._path):
				os.mkdir(self._path)
		except Exception:
			raise BackendError('Unable to create sandbox base directory "%s"!' % self._path)

	def get_path(self):
		return self._path

	def get_sandbox(self, gcID):
		# Speed up function by caching result of listdir
		def searchSandbox(source):
			for path in imap(lambda sbox: os.path.join(self._path, sbox), source):
				if os.path.exists(os.path.join(path, gcID)):
					return path
		result = searchSandbox(self._cache)
		if result:
			return result
		oldCache = self._cache[:]
		self._cache = lfilter(lambda x: os.path.isdir(os.path.join(self._path, x)), os.listdir(self._path))
		return searchSandbox(ifilter(lambda x: x not in oldCache, self._cache))


class LocalPurgeJobs(CancelJobs):
	def __init__(self, config, sandbox_helper):
		CancelJobs.__init__(self, config)
		self._sandbox_helper = sandbox_helper

	def execute(self, wmsIDs, wmsName): # yields list of (wmsID, job_status, job_info)
		activity = utils.ActivityLog('waiting for jobs to finish')
		time.sleep(5)
		for wmsID in wmsIDs:
			path = self._sandbox_helper.get_sandbox('WMSID.%s.%s' % (wmsName, wmsID))
			if path is None:
				self._log.warning('Sandbox for job %r could not be found', wmsID)
				continue
			try:
				shutil.rmtree(path)
			except Exception:
				raise BackendError('Sandbox for job %r could not be deleted', wmsID)
			yield wmsID
		activity.finish()


class LocalWMS(BasicWMS):
	configSections = BasicWMS.configSections + ['local']

	def __init__(self, config, name, submitExec, checkExecutor, cancelExecutor):
		config.set('broker', 'RandomBroker')
		config.setInt('wait idle', 20)
		config.setInt('wait work', 5)
		self.submitExec = submitExec
		self._sandbox_helper = SandboxHelper(config)
		BasicWMS.__init__(self, config, name, checkExecutor = checkExecutor,
			cancelExecutor = CancelAndPurgeJobs(config, cancelExecutor, LocalPurgeJobs(config, self._sandbox_helper)))

		self.brokerSite = config.getPlugin('site broker', 'UserBroker', cls = Broker,
			inherit = True, tags = [self], pargs = ('sites', 'sites', self.getNodes))
		self.brokerQueue = config.getPlugin('queue broker', 'UserBroker', cls = Broker,
			inherit = True, tags = [self], pargs = ('queue', 'queues', self.getQueues))

		self.scratchPath = config.getList('scratch path', ['TMPDIR', '/tmp'], onChange = True)
		self.submitOpts = config.get('submit options', '', onChange = None)
		self.memory = config.getInt('memory', -1, onChange = None)


	# Submit job and yield (jobNum, WMS ID, other data)
	def _submitJob(self, jobNum, module):
		activity = utils.ActivityLog('submitting jobs')

		try:
			sandbox = tempfile.mkdtemp('', '%s.%04d.' % (module.taskID, jobNum), self._sandbox_helper.get_path())
		except Exception:
			raise BackendError('Unable to create sandbox directory "%s"!' % sandbox)
		sbPrefix = sandbox.replace(self._sandbox_helper.get_path(), '').lstrip('/')
		def translateTarget(d, s, t):
			return (d, s, os.path.join(sbPrefix, t))
		self.smSBIn.doTransfer(ismap(translateTarget, self._getSandboxFilesIn(module)))

		self._writeJobConfig(os.path.join(sandbox, '_jobconfig.sh'), jobNum, module, {
			'GC_SANDBOX': sandbox, 'GC_SCRATCH_SEARCH': str.join(' ', self.scratchPath)})
		reqs = self.brokerSite.brokerAdd(module.getRequirements(jobNum), WMS.SITES)
		reqs = dict(self.brokerQueue.brokerAdd(reqs, WMS.QUEUES))
		if (self.memory > 0) and (reqs.get(WMS.MEMORY, 0) < self.memory):
			reqs[WMS.MEMORY] = self.memory # local jobs need higher (more realistic) memory requirements

		(stdout, stderr) = (os.path.join(sandbox, 'gc.stdout'), os.path.join(sandbox, 'gc.stderr'))
		jobName = module.getDescription(jobNum).jobName
		proc = utils.LoggedProcess(self.submitExec, '%s %s "%s" %s' % (self.submitOpts,
			self.getSubmitArguments(jobNum, jobName, reqs, sandbox, stdout, stderr),
			utils.pathShare('gc-local.sh'), self.getJobArguments(jobNum, sandbox)))
		retCode = proc.wait()
		wmsIdText = proc.getOutput().strip().strip('\n')
		try:
			wmsId = self.parseSubmitOutput(wmsIdText)
		except Exception:
			wmsId = None

		del activity

		if retCode != 0:
			self._log.warning('%s failed:', self.submitExec)
		elif wmsId is None:
			self._log.warning('%s did not yield job id:\n%s', self.submitExec, wmsIdText)
		if wmsId:
			wmsId = self._createId(wmsId)
			open(os.path.join(sandbox, wmsId), 'w')
		else:
			proc.logError(self.errorLog)
		return (jobNum, utils.QM(wmsId, wmsId, None), {'sandbox': sandbox})


	def _getJobsOutput(self, ids):
		if not len(ids):
			raise StopIteration

		activity = utils.ActivityLog('retrieving job outputs')
		for wmsId, jobNum in ids:
			path = self._sandbox_helper.get_sandbox(wmsId)
			if path is None:
				yield (jobNum, None)
				continue

			# Cleanup sandbox
			outFiles = lchain(imap(lambda pat: glob.glob(os.path.join(path, pat)), self.outputFiles))
			utils.removeFiles(ifilter(lambda x: x not in outFiles, imap(lambda fn: os.path.join(path, fn), os.listdir(path))))

			yield (jobNum, path)
		del activity


	def _getSandboxFiles(self, module, monitor, smList):
		files = BasicWMS._getSandboxFiles(self, module, monitor, smList)
		for idx, authFile in enumerate(self._token.getAuthFiles()):
			files.append(VirtualFile(('_proxy.dat.%d' % idx).replace('.0', ''), open(authFile, 'r').read()))
		return files


	def getQueues(self):
		return None

	def getNodes(self):
		return None

	def checkReq(self, reqs, req, test = lambda x: x > 0):
		if req in reqs:
			return test(reqs[req])
		return False

	def getJobArguments(self, jobNum, sandbox):
		raise AbstractError

	def getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr):
		raise AbstractError

	def parseSubmitOutput(self, data):
		raise AbstractError


class Local(WMS):
	configSections = WMS.configSections + ['local']

	def __new__(cls, config, name):
		def createWMS(wms):
			try:
				wmsCls = WMS.getClass(wms)
			except Exception:
				raise BackendError('Unable to load backend class %s' % repr(wms))
			wms_config = config.changeView(viewClass = 'TaggedConfigView', setClasses = [wmsCls])
			return WMS.createInstance(wms, wms_config, name)
		wms = config.get('wms', '')
		if wms:
			return createWMS(wms)
		ec = ExceptionCollector()
		for cmd, wms in [('sacct', 'SLURM'), ('sgepasswd', 'OGE'), ('pbs-config', 'PBS'),
				('qsub', 'OGE'), ('bsub', 'LSF'), ('job_slurm', 'JMS')]:
			try:
				utils.resolveInstallPath(cmd)
			except Exception:
				ec.collect()
				continue
			return createWMS(wms)
		ec.raise_any(BackendError('No valid local backend found!')) # at this point all backends have failed!
