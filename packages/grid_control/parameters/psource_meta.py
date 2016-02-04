#-#  Copyright 2012-2015 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.parameters.psource_base import ParameterSource
from python_compat import md5

def combineSyncResult(a, b, sc_fun = lambda x, y: x or y):
	if a is None:
		return b
	(redo_a, disable_a, sizeChange_a) = a
	(redo_b, disable_b, sizeChange_b) = b
	redo_a.update(redo_b)
	disable_a.update(disable_b)
	return (redo_a, disable_a, sc_fun(sizeChange_a, sizeChange_b))

class ForwardingParameterSource(ParameterSource):
	def __init__(self, psource):
		ParameterSource.__init__(self)
		self._psource = psource

	def getMaxParameters(self):
		return self._psource.getMaxParameters()

	def fillParameterKeys(self, result):
		self._psource.fillParameterKeys(result)

	def fillParameterInfo(self, pNum, result):
		self._psource.fillParameterInfo(pNum, result)

	def resync(self):
		return self._psource.resync()

	def show(self, level = 0, other = ''):
		ParameterSource.show(self, level, other)
		self._psource.show(level + 1)

	def getHash(self):
		return self._psource.getHash()


class RangeParameterSource(ForwardingParameterSource):
	def __init__(self, psource, posStart = None, posEnd = None):
		ForwardingParameterSource.__init__(self, psource)
		self._posStart = utils.QM(posStart is None, 0, posStart)
		self._posEndUser = posEnd
		self._posEnd = utils.QM(self._posEndUser is None, self._psource.getMaxParameters() - 1, self._posEndUser)

	def getMaxParameters(self):
		return self._posEnd - self._posStart + 1

	def getHash(self):
		return md5(self._psource.getHash() + str([self._posStart, self._posEnd])).hexdigest()

	def fillParameterInfo(self, pNum, result):
		self._psource.fillParameterInfo(pNum + self._posStart, result)

	def resync(self):
		(result_redo, result_disable, result_sizeChange) = self.resyncCreate()
		(psource_redo, psource_disable, psource_sizeChange) = self._psource.resync()
		for pNum in psource_redo:
			if (pNum >= self._posStart) and (pNum <= self._posEnd):
				result_redo.add(pNum - self._posStart)
		for pNum in psource_disable:
			if (pNum >= self._posStart) and (pNum <= self._posEnd):
				result_disable.add(pNum - self._posStart)
		oldPosEnd = self._posEnd
		self._posEnd = utils.QM(self._posEndUser is None, self._psource.getMaxParameters() - 1, self._posEndUser)
		return (result_redo, result_disable, result_sizeChange or (oldPosEnd != self._posEnd))

	def show(self, level = 0):
		ForwardingParameterSource.show(self, level, 'range = (%s, %s)' % (self._posStart, self._posEnd))
ParameterSource.managerMap['range'] = 'RangeParameterSource'


# Meta processing of parameter psources
class BaseMultiParameterSource(ParameterSource):
	def __init__(self, *psources):
		ParameterSource.__init__(self)
		self._psourceList = psources
		self._psourceMaxList = map(lambda p: p.getMaxParameters(), self._psourceList)
		self._maxParameters = self.initMaxParameters()

	def getMaxParameters(self):
		return self._maxParameters

	def initMaxParameters(self):
		raise AbstractError

	def fillParameterKeys(self, result):
		for psource in self._psourceList:
			psource.fillParameterKeys(result)

	def resync(self):
		(result_redo, result_disable, result_sizeChange) = self.resyncCreate()
		self._psourceMaxList = map(lambda p: p.getMaxParameters(), self._psourceList)
		oldMaxParameters = self._maxParameters
		self._maxParameters = self.initMaxParameters()
		return (result_redo, result_disable, result_sizeChange or (oldMaxParameters != self._maxParameters))

	def show(self, level = 0):
		ParameterSource.show(self, level)
		for psource in self._psourceList:
			psource.show(level + 1)

	def getHash(self):
		return md5(str(map(lambda p: str(p.getMaxParameters()) + p.getHash(), self._psourceList))).hexdigest()


# Aggregates and propagates results and changes to psources
class MultiParameterSource(BaseMultiParameterSource):
	# Get local parameter numbers (result) from psource index (pIdx) and subpsource parameter number (pNum)
	def translateNum(self, pIdx, pNum):
		raise AbstractError

	def resync(self):
		psource_resyncList = map(lambda p: p.resync(), self._psourceList)
		(result_redo, result_disable, result_sizeChange) = BaseMultiParameterSource.resync(self)
		for (idx, psource_resync) in enumerate(psource_resyncList):
			(psource_redo, psource_disable, psource_sizeChange) = psource_resync
			for pNum in psource_redo:
				result_redo.update(self.translateNum(idx, pNum))
			for pNum in psource_disable:
				result_disable.update(self.translateNum(idx, pNum))
		return (result_redo, result_disable, result_sizeChange)


# Base class for psources invoking their sub-psources in parallel
class BaseZipParameterSource(MultiParameterSource):
	def fillParameterInfo(self, pNum, result):
		for (psource, maxN) in zip(self._psourceList, self._psourceMaxList):
			if maxN is not None:
				if pNum < maxN:
					psource.fillParameterInfo(pNum, result)
			else:
				psource.fillParameterInfo(pNum, result)

	def resync(self): # Quicker version than the general purpose implementation
		result = self.resyncCreate()
		for psource in self._psourceList:
			result = combineSyncResult(result, psource.resync())
		oldMaxParameters = self._maxParameters
		self._maxParameters = self.initMaxParameters()
		return (result[0], result[1], oldMaxParameters != self._maxParameters)


class ZipShortParameterSource(BaseZipParameterSource):
	def initMaxParameters(self):
		maxN = filter(lambda n: n is not None, self._psourceMaxList)
		if len(maxN):
			return min(maxN)

class ZipLongParameterSource(BaseZipParameterSource):
	def initMaxParameters(self):
		maxN = filter(lambda n: n is not None, self._psourceMaxList)
		if len(maxN):
			return max(maxN)

	def __repr__(self):
		return 'zip(%s)' % str.join(', ', map(repr, self._psourceList))
ParameterSource.managerMap['zip'] = 'ZipLongParameterSource'


class ChainParameterSource(MultiParameterSource):
	def initMaxParameters(self):
		self.offsetList = map(lambda pIdx: sum(self._psourceMaxList[:pIdx]), range(len(self._psourceList)))
		return sum(self._psourceMaxList)

	def translateNum(self, pIdx, pNum):
		return [pNum + self.offsetList[pIdx]]

	def fillParameterInfo(self, pNum, result):
		limit = 0
		for (psource, maxN) in zip(self._psourceList, self._psourceMaxList):
			if pNum < limit + maxN:
				return psource.fillParameterInfo(pNum - limit, result)
			limit += maxN

	def __repr__(self):
		return 'chain(%s)' % str.join(', ', map(repr, self._psourceList))
ParameterSource.managerMap['chain'] = 'ChainParameterSource'


class RepeatParameterSource(ChainParameterSource):
	def __init__(self, psource, times):
		self._psource = psource
		self.times = times
		MultiParameterSource.__init__(self, psource)

	def initMaxParameters(self):
		self.maxN = self._psource.getMaxParameters()
		if self.maxN is not None:
			return self.times * self.maxN
		return self.times

	def translateNum(self, pIdx, pNum):
		return map(lambda i: pNum + i * self.maxN, range(self.times))

	def fillParameterInfo(self, pNum, result):
		self._psource.fillParameterInfo(pNum % self.maxN, result)

	def show(self, level = 0):
		ParameterSource.show(self, level, 'times = %d' % self.times)
		self._psource.show(level + 1)

	def getHash(self):
		return md5(self._psource.getHash() + str(self.times)).hexdigest()

	def __repr__(self):
		return 'repeat(%s, %d)' % (repr(self._psource), self.times)
ParameterSource.managerMap['repeat'] = 'RepeatParameterSource'


class CrossParameterSource(MultiParameterSource):
	def initMaxParameters(self):
		self.quickFill = []
		prev = 1
		for (psource, maxN) in zip(self._psourceList, self._psourceMaxList):
			self.quickFill.append((psource, maxN, prev))
			if maxN:
				prev *= maxN
		maxList = filter(lambda n: n is not None, self._psourceMaxList)
		if maxList:
			return reduce(lambda a, b: a * b, maxList)

	def translateNum(self, pIdx, pNum):
		psource, maxN, prev = self.quickFill[pIdx]
		return filter(lambda x: (x / prev) % maxN == pNum, range(self.getMaxParameters()))

	def fillParameterInfo(self, pNum, result):
		for (psource, maxN, prev) in self.quickFill:
			if maxN:
				psource.fillParameterInfo((pNum / prev) % maxN, result)
			else:
				psource.fillParameterInfo(pNum, result)

	def __repr__(self):
		return 'cross(%s)' % str.join(', ', map(repr, self._psourceList))
ParameterSource.managerMap['cross'] = 'CrossParameterSource'


class ErrorParameterSource(ChainParameterSource):
	def __init__(self, *psources):
		self.rawpsources = psources
		central = map(lambda p: RangeParameterSource(p, 0, 0), psources)
		chain = [ZipLongParameterSource(*central)]
		for pidx, p in enumerate(psources):
			if p.getMaxParameters() is not None:
				tmp = list(central)
				tmp[pidx] = RangeParameterSource(psources[pidx], 1, None)
				chain.append(CrossParameterSource(*tmp))
		ChainParameterSource.__init__(self, *chain)

	def fillParameterKeys(self, result):
		for psource in self.rawpsources:
			psource.fillParameterKeys(result)
ParameterSource.managerMap['variation'] = 'ErrorParameterSource'


class CombineParameterSource(ZipLongParameterSource):
	# combine according to common parameter value
	pass
ParameterSource.managerMap['combine'] = 'CombineParameterSource'
