#-#  Copyright 2012-2014 Karlsruhe Institute of Technology
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

import csv, gzip
from grid_control import utils
from grid_control.parameters.psource_base import ParameterInfo, ParameterMetadata, ParameterSource
from grid_control.parameters.psource_basic import InternalParameterSource
from python_compat import sorted

# Reader for grid-control dump files
class GCDumpParameterSource(ParameterSource):
	def __init__(self, fn):
		ParameterSource.__init__(self)
		fp = gzip.open(fn, 'rb')
		keyline = fp.readline().lstrip('#').strip()
		self.keys = []
		if keyline:
			self.keys = eval(keyline)
		def parseLine(line):
			if not line.startswith('#'):
				pNumStr, stored = map(str.strip, line.split('\t', 1))
				return ('!' in pNumStr, int(pNumStr.rstrip('!')), map(eval, stored.split('\t')))
		self.values = map(parseLine, fp.readlines())

	def getMaxParameters(self):
		return len(self.values)

	def fillParameterKeys(self, result):
		result.extend(map(lambda k: ParameterMetadata(k, untracked = False), self.keys))

	def fillParameterInfo(self, pNum, result):
		result[ParameterInfo.ACTIVE] = not self.values[pNum][0]
		result.update(filter(lambda (k, v): v is not None, zip(self.keys, self.values[pNum][2])))

	def write(cls, fn, pa):
		fp = gzip.open(fn, 'wb')
		keys = sorted(filter(lambda p: p.untracked == False, pa.getJobKeys()))
		fp.write('# %s\n' % keys)
		maxN = pa.getMaxJobs()
		if maxN:
			log = None
			for jobNum in range(maxN):
				del log
				log = utils.ActivityLog('Writing parameter dump [%d/%d]' % (jobNum + 1, maxN))
				meta = pa.getJobInfo(jobNum)
				if meta.get(ParameterInfo.ACTIVE, True):
					fp.write('%d\t%s\n' % (jobNum, str.join('\t', map(lambda k: repr(meta.get(k, '')), keys))))
				else:
					fp.write('%d!\t%s\n' % (jobNum, str.join('\t', map(lambda k: repr(meta.get(k, '')), keys))))
	write = classmethod(write)


# Reader for CSV files
class CSVParameterSource(InternalParameterSource):
	def __init__(self, fn, format = 'sniffed'):
		sniffed = csv.Sniffer().sniff(open(fn).read(1024))
		csv.register_dialect('sniffed', sniffed)
		tmp = list(csv.DictReader(open(fn), dialect = format))

		def cleanupDict(d):
			# strip all key value entries
			tmp = tuple(map(lambda item: map(str.strip, item), d.items()))
			# filter empty parameters
			return filter(lambda (k, v): k != '', tmp)
		keys = []
		if len(tmp):
			keys = map(ParameterMetadata, tmp[0].keys())
		values = map(lambda d: dict(cleanupDict(d)), tmp)
		InternalParameterSource.__init__(self, values, keys)

	def create(cls, pconfig = None, src = 'CSV'):
		fn = pconfig.get(src, 'source')
		return CSVParameterSource(fn, pconfig.get(src, 'format', 'sniffed'))
	create = classmethod(create)
ParameterSource.managerMap['csv'] = 'CSVParameterSource'
