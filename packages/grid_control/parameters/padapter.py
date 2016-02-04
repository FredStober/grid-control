#-#  Copyright 2013-2015 Karlsruhe Institute of Technology
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

import os, gzip
from grid_control import utils
from grid_control.parameters.psource_base import ParameterInfo, ParameterMetadata, ParameterSource
from grid_control.parameters.psource_file import GCDumpParameterSource
from hpfwk import APIError, Plugin
from python_compat import md5, set, sorted

class ParameterAdapter(Plugin):
	def __init__(self, config, source):
		self._source = source
		self._prune = True

	def getMaxJobs(self):
		return self._source.getMaxParameters()

	def getJobKeys(self):
		result = map(lambda k: ParameterMetadata(k, untracked=True), ['GC_JOB_ID', 'GC_PARAM'])
		self._source.fillParameterKeys(result)
		return result

	def getJobInfo(self, jobNum, pNum = None):
		if pNum is None:
			pNum = jobNum
		if jobNum is None:
			raise APIError('Unable to process jobNum None!')
		result = {ParameterInfo.ACTIVE: True, ParameterInfo.REQS: []}
		result['GC_JOB_ID'] = jobNum
		result['GC_PARAM'] = pNum
		self._source.fillParameterInfo(pNum, result)
		if self._prune:
			old = dict(result)
			result = utils.filterDict(result, vF = lambda v: v != '')
		return result

	def canSubmit(self, jobNum): # Use caching to speed up job manager operations
		return self.getJobInfo(jobNum)[ParameterInfo.ACTIVE]

	def resync(self):
		return self._source.resync()

	def show(self):
		self._source.show()


class BasicParameterAdapter(ParameterAdapter):
	def __init__(self, config, source):
		ParameterAdapter.__init__(self, config, source)
		self._activeMap = {}
		self._resyncState = None

	def canSubmit(self, jobNum): # Use caching to speed up job manager operations
		if jobNum not in self._activeMap:
			self._activeMap[jobNum] = ParameterAdapter.canSubmit(self, jobNum)
		return self._activeMap[jobNum]

	def resync(self): # Allow queuing of resync results - (because of external or init trigger)
		if (self._resyncState is None):
			self._resyncInternal()
		result = self._resyncState
		if result is not None:
			self._activeMap = {} # invalidate cache on changes
		self._resyncState = None
		return result

	def _resyncInternal(self):
		self._resyncState = self._source.resync()


class TrackedParameterAdapter(BasicParameterAdapter):
	def __init__(self, config, source):
		self._rawSource = source
		BasicParameterAdapter.__init__(self, config, source)
		self._mapJob2PID = {}
		self._pathJob2PID = config.getWorkPath('params.map.gz')
		self._pathParams = config.getWorkPath('params.dat.gz')

		# Find out if init should be performed - overrides userResync!
		userInit = config.getState('init', detail = 'parameters')
		needInit = False
		if not (os.path.exists(self._pathParams) and os.path.exists(self._pathJob2PID)):
			needInit = True # Init needed if no parameter log exists
		if userInit and not needInit and (source.getMaxParameters() is not None):
			utils.eprint('Re-Initialization will overwrite the current mapping between jobs and parameter/dataset content! This can lead to invalid results!')
			if utils.getUserBool('Do you want to perform a syncronization between the current mapping and the new one to avoid this?', True):
				userInit = False
		doInit = userInit or needInit

		# Find out if resync should be performed
		userResync = config.getState('resync', detail = 'parameters')
		needResync = False
		pHash = self._rawSource.getHash()
		self.storedHash = config.get('parameter hash', pHash, persistent = True)
		if self.storedHash != pHash:
			needResync = True # Resync needed if parameters have changed
		doResync = (userResync or needResync) and not doInit

		if not doResync and not doInit: # Reuse old mapping
			activity = utils.ActivityLog('Loading cached parameter information')
			self.readJob2PID()
		elif doResync: # Perform sync
			activity = utils.ActivityLog('Syncronizing parameter information')
			self.storedHash = None
			self._resyncInternal()
		elif doInit: # Write current state
			self.writeJob2PID(self._pathJob2PID)
			GCDumpParameterSource.write(self._pathParams, self)

	def readJob2PID(self):
		fp = gzip.open(self._pathJob2PID, 'r')
		self.maxN = int(fp.readline())
		if not self.maxN:
			self.maxN = None
		mapInfo = filter(lambda x: x, map(str.strip, fp.readline().split(',')))
		self._mapJob2PID = dict(map(lambda x: tuple(map(lambda y: int(y.lstrip('!')), x.split(':'))), mapInfo))
		self._activeMap = {}

	def writeJob2PID(self, fn):
		fp = gzip.open(fn, 'w')
		fp.write('%d\n' % max(0, self._rawSource.getMaxParameters()))
		data = filter(lambda (jobNum, pNum): jobNum != pNum, self._mapJob2PID.items())
		datastr = map(lambda (jobNum, pNum): '%d:%d' % (jobNum, pNum), data)
		fp.write('%s\n' % str.join(',', datastr))

	def getJobInfo(self, jobNum): # Perform mapping between jobNum and parameter number
		pNum = self._mapJob2PID.get(jobNum, jobNum)
		if (pNum < self._source.getMaxParameters()) or (self._source.getMaxParameters() is None):
			result = BasicParameterAdapter.getJobInfo(self, jobNum, pNum)
		else:
			result = {ParameterInfo.ACTIVE: False}
		result['GC_JOB_ID'] = jobNum
		return result

	def _resyncInternal(self): # This function is _VERY_ time critical!
		tmp = self._rawSource.resync() # First ask about psource changes
		(redo, disable, sizeChange) = (set(tmp[0]), set(tmp[1]), tmp[2])
		hashNew = self._rawSource.getHash()
		hashChange = self.storedHash != hashNew
		self.storedHash = hashNew
		if not (redo or disable or sizeChange or hashChange):
			self._resyncState = None
			return 

		def translatePSource(psource): # Reduces psource output to essential information for diff
			keys_store = sorted(filter(lambda k: k.untracked == False, psource.getJobKeys()))
			def translateEntry(meta): # Translates parameter setting into hash
				tmp = md5()
				for key in filter(lambda k: k in meta, keys_store):
					if str(meta[key]):
						tmp.update(key)
						tmp.update(str(meta[key]))
				return { ParameterInfo.HASH: tmp.hexdigest(), 'GC_PARAM': meta['GC_PARAM'],
					ParameterInfo.ACTIVE: meta[ParameterInfo.ACTIVE] }
			if psource.getMaxJobs() is not None:
				for jobNum in range(psource.getMaxJobs()):
					yield translateEntry(psource.getJobInfo(jobNum))

		old = ParameterAdapter(None, GCDumpParameterSource(self._pathParams))
		params_old = list(translatePSource(old))
		new = ParameterAdapter(None, self._rawSource)
		params_new = list(translatePSource(new))

		mapJob2PID = {}
		def sameParams(paramsAdded, paramsMissing, paramsSame, oldParam, newParam):
			if not oldParam[ParameterInfo.ACTIVE] and newParam[ParameterInfo.ACTIVE]:
				redo.add(newParam['GC_PARAM'])
			if oldParam[ParameterInfo.ACTIVE] and not newParam[ParameterInfo.ACTIVE]:
				disable.add(newParam['GC_PARAM'])
			mapJob2PID[oldParam['GC_PARAM']] = newParam['GC_PARAM']
		(pAdded, pMissing, pSame) = utils.DiffLists(params_old, params_new,
			lambda a, b: cmp(a[ParameterInfo.HASH], b[ParameterInfo.HASH]), sameParams)

		# Construct complete parameter space psource with missing parameter entries and intervention state
		# NNNNNNNNNNNNN OOOOOOOOO | source: NEW (==self) and OLD (==from file)
		# <same><added> <missing> | same: both in NEW and OLD, added: only in NEW, missing: only in OLD
		oldMaxJobs = old.getMaxJobs()
		# assign sequential job numbers to the added parameter entries
		pAdded.sort(key = lambda x: x['GC_PARAM'])
		for (idx, meta) in enumerate(pAdded):
			if oldMaxJobs + idx != meta['GC_PARAM']:
				mapJob2PID[oldMaxJobs + idx] = meta['GC_PARAM']

		missingInfos = []
		newMaxJobs = new.getMaxJobs()
		pMissing.sort(key = lambda x: x['GC_PARAM'])
		for (idx, meta) in enumerate(pMissing):
			mapJob2PID[meta['GC_PARAM']] = newMaxJobs + idx
			tmp = old.getJobInfo(newMaxJobs + idx, meta['GC_PARAM'])
			tmp.pop('GC_PARAM')
			if tmp[ParameterInfo.ACTIVE]:
				tmp[ParameterInfo.ACTIVE] = False
				disable.add(newMaxJobs + idx)
			missingInfos.append(tmp)

		if missingInfos:
			from grid_control.parameters.psource_meta import ChainParameterSource
			from grid_control.parameters.psource_basic import InternalParameterSource
			currentInfoKeys = new.getJobKeys()
			missingInfoKeys = filter(lambda key: key not in currentInfoKeys, old.getJobKeys())
			self._source = ChainParameterSource(self._rawSource, InternalParameterSource(missingInfos, missingInfoKeys))

		self._mapJob2PID = mapJob2PID # Update Job2PID map
		redo = redo.difference(disable)
		if redo or disable:
			mapPID2Job = dict(map(lambda (k, v): (v, k), self._mapJob2PID.items()))
			translate = lambda pNum: mapPID2Job.get(pNum, pNum)
			self._resyncState = (set(map(translate, redo)), set(map(translate, disable)), sizeChange)
		elif sizeChange:
			self._resyncState = (set(), set(), sizeChange)
		# Write resynced state
		self.writeJob2PID(self._pathJob2PID + '.old')
		GCDumpParameterSource.write(self._pathParams + '.old', self)
		os.rename(self._pathJob2PID + '.old', self._pathJob2PID)
		os.rename(self._pathParams + '.old', self._pathParams)
