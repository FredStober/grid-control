#-#  Copyright 2009-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, time, logging
from grid_control import utils
from grid_control.gc_plugin import NamedPlugin
from grid_control.job_db import Job

class EventHandler(NamedPlugin):
	configSections = NamedPlugin.configSections + ['events']
	tagName = 'event'

	def __init__(self, config, name, task, submodules = []):
		NamedPlugin.__init__(self, config, name)
		self._log = logging.getLogger('monitoring')
		(self.config, self.task, self.submodules) = (config, task, submodules)

	def onJobSubmit(self, wms, jobObj, jobNum):
		for submodule in self.submodules:
			submodule.onJobSubmit(wms, jobObj, jobNum)

	def onJobUpdate(self, wms, jobObj, jobNum, data):
		for submodule in self.submodules:
			submodule.onJobUpdate(wms, jobObj, jobNum, data)

	def onJobOutput(self, wms, jobObj, jobNum, retCode):
		for submodule in self.submodules:
			submodule.onJobOutput(wms, jobObj, jobNum, retCode)

	def onTaskFinish(self, nJobs):
		for submodule in self.submodules:
			submodule.onTaskFinish(nJobs)


# Monitoring base class with submodule support
class Monitoring(EventHandler):
	tagName = 'monitor'

	# Script to call later on
	def getScript(self):
		return utils.listMapReduce(lambda m: list(m.getScript()), self.submodules)

	def getTaskConfig(self):
		tmp = {'GC_MONITORING': str.join(' ', map(os.path.basename, self.getScript()))}
		return utils.mergeDicts(map(lambda m: m.getTaskConfig(), self.submodules) + [tmp])

	def getFiles(self):
		return utils.listMapReduce(lambda m: list(m.getFiles()), self.submodules, self.getScript())


class MultiMonitor(Monitoring):
	def __init__(self, config, name, monitoringProxyList, task):
		submoduleList = map(lambda m: m.getInstance(task), monitoringProxyList)
		Monitoring.__init__(self, config, name, None, submoduleList)


class ScriptMonitoring(Monitoring):
	alias = ['scripts']
	configSections = EventHandler.configSections + ['scripts']

	def __init__(self, config, name, task):
		Monitoring.__init__(self, config, name, task)
		self.silent = config.getBool('silent', True, onChange = None)
		self.evtSubmit = config.getCommand('on submit', '', onChange = None)
		self.evtStatus = config.getCommand('on status', '', onChange = None)
		self.evtOutput = config.getCommand('on output', '', onChange = None)
		self.evtFinish = config.getCommand('on finish', '', onChange = None)
		self.running = {}
		self.runningToken = 0
		self.runningMax = config.getTime('script runtime', 5, onChange = None)

	def cleanupRunning(self):
		currentTime = time.time()
		for (token, startTime) in list(self.running.items()):
			if currentTime - startTime > self.runningMax:
				self.running.pop(token, None) # lock free: ignore missing tokens

	# Get both task and job config / state dicts
	def scriptThread(self, token, script, jobNum = None, jobObj = None, allDict = {}):
		try:
			tmp = {}
			if jobNum is not None:
				tmp.update(self.task.getSubmitInfo(jobNum))
			if jobObj is not None:
				tmp.update(jobObj.getAll())
			tmp['WORKDIR'] = self.config.getWorkPath()
			tmp.update(self.task.getTaskConfig())
			if jobNum is not None:
				tmp.update(self.task.getJobConfig(jobNum))
				tmp.update(self.task.getSubmitInfo(jobNum))
			tmp.update(allDict)
			for key, value in tmp.iteritems():
				if not key.startswith('GC_'):
					key = 'GC_' + key
				os.environ[key] = str(value)

			script = self.task.substVars(script, jobNum, tmp)
			if self.silent:
				utils.LoggedProcess(script).wait()
			else:
				os.system(script)
		except Exception:
			self._log.exception('Error while running user script!')
		self.running.pop(token, None)

	def runInBackground(self, script, jobNum = None, jobObj = None, addDict =  {}):
		if script != '':
			self.runningToken += 1
			self.running[self.runningToken] = time.time()
			utils.gcStartThread('Running monitoring script %s' % script,
				self.scriptThread, self.runningToken, script, jobNum, jobObj)
			self.cleanupRunning()

	# Called on job submission
	def onJobSubmit(self, wms, jobObj, jobNum):
		self.runInBackground(self.evtSubmit, jobNum, jobObj)

	# Called on job status update
	def onJobUpdate(self, wms, jobObj, jobNum, data):
		self.runInBackground(self.evtStatus, jobNum, jobObj, {'STATUS': Job.enum2str(jobObj.state)})

	# Called on job status update
	def onJobOutput(self, wms, jobObj, jobNum, retCode):
		self.runInBackground(self.evtOutput, jobNum, jobObj, {'RETCODE': retCode})

	# Called at the end of the task
	def onTaskFinish(self, nJobs):
		self.runInBackground(self.evtFinish, addDict = {'NJOBS': nJobs})
		while self.running:
			self.cleanupRunning()
			time.sleep(0.1)
