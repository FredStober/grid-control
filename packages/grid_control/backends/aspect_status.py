# | Copyright 2016 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.backends.backend_tools import BackendError, BackendExecutor
from grid_control.job_db import Job
from grid_control.utils.data_structures import makeEnum
from hpfwk import AbstractError

CheckInfo = makeEnum(['WMSID', 'RAW_STATUS', 'QUEUE', 'WN', 'SITE'])

class CheckJobs(BackendExecutor):
	def execute(self, wmsIDs): # yields list of (wmsID, job_status, job_info)
		raise AbstractError


class CheckJobsWithProcess(CheckJobs):
	def __init__(self, config, proc_factory, status_map = None):
		CheckJobs.__init__(self, config)
		self._timeout = config.getInt('check timeout', 60, onChange = None)
		self._errormsg = 'Job status command returned with exit code %(code)s'
		(self._proc_factory, self._status_map) = (proc_factory, status_map or {})

	def execute(self, wmsIDs): # yields list of (wmsID, job_status, job_info)
		proc = self._proc_factory.create_proc(wmsIDs)
		for job_info in self._parse(proc):
			if job_info and not utils.abort():
				yield self._parse_job_info(job_info)
		if proc.status(timeout = 0, terminate = True) != 0:
			self._handleError(proc)

	def _parse(self, proc): # return job_info(s)
		raise AbstractError

	def _parse_status(self, value, default):
		return self._status_map.get(value, default)

	def _parse_job_info(self, job_info): # return (wmsID, job_status, job_info)
		try:
			job_status = self._parse_status(job_info.get(CheckInfo.RAW_STATUS), Job.UNKNOWN)
			return (job_info.pop(CheckInfo.WMSID), job_status, job_info)
		except Exception:
			raise BackendError('Unable to parse job info %s' % repr(job_info))

	def _handleError(self, proc):
		self._filter_proc_log(proc, self._errormsg)
