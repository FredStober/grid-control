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

from grid_control.datasets.dproc_base import DataProcessor, NullDataProcessor
from grid_control.datasets.provider_base import DataProvider, DatasetError
from hpfwk import ExceptionCollector
from python_compat import imap, reduce, set


class MultiDatasetProvider(DataProvider):
	def __init__(self, config, datasource_name, dataset_expr, dataset_nick, providerList):
		DataProvider.__init__(self, config, datasource_name, dataset_expr, dataset_nick)
		self._stats = DataProcessor.create_instance('SimpleStatsDataProcessor', config, 'dataset', None, self._log, 'Summary: Running over ')
		self._provider_list = providerList


	def queryLimit(self):
		return max(imap(lambda x: x.queryLimit(), self._provider_list))


	def checkSplitter(self, splitter):
		def getProposal(x):
			return reduce(lambda prop, prov: prov.checkSplitter(prop), self._provider_list, x)
		if getProposal(splitter) != getProposal(getProposal(splitter)):
			raise DatasetError('Dataset providers could not agree on valid dataset splitter!')
		return getProposal(splitter)


	def get_dataset_expr(self):
		return str.join(' ', imap(lambda p: p.get_dataset_expr(), self._provider_list))


	def getDatasets(self):
		if self._cache_dataset is None:
			self._cache_dataset = set()
			ec = ExceptionCollector()
			for provider in self._provider_list:
				try:
					self._cache_dataset.update(provider.getDatasets())
				except Exception:
					ec.collect()
			ec.raise_any(DatasetError('Could not retrieve all datasets!'))
		return list(self._cache_dataset)


	def getBlocks(self, show_stats):
		statsProcessor = NullDataProcessor()
		if show_stats:
			statsProcessor = self._stats
		if self._cache_block is None:
			ec = ExceptionCollector()
			def getAllBlocks():
				for provider in self._provider_list:
					try:
						for block in provider.get_blocks_raw():
							yield block
					except Exception:
						ec.collect()
			try:
				self._cache_block = list(statsProcessor.process(self._dataset_processor.process(getAllBlocks())))
			except Exception:
				raise DatasetError('Unable to run datasets through processing pipeline!')
			ec.raise_any(DatasetError('Could not retrieve all datasets!'))
			self._raise_on_abort()
		return self._cache_block
