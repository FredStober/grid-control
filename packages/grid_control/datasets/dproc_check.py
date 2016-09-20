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

from grid_control.datasets.dproc_base import DataProcessor
from grid_control.datasets.provider_base import DataProvider, DatasetError
from grid_control.utils.data_structures import make_enum
from python_compat import imap, md5_hex, set


# Enum to specify how to react to multiple occurences of something
DatasetUniqueMode = make_enum(['warn', 'abort', 'skip', 'ignore', 'record'])
DatasetCheckMode = make_enum(['warn', 'abort', 'ignore'])


class DataChecker(DataProcessor):
	def _handle_error(self, msg, mode):
		if mode == DatasetCheckMode.warn:
			self._log.warning(msg)
		elif mode == DatasetCheckMode.abort:
			raise DatasetError(msg)


class EntriesConsistencyDataProcessor(DataChecker):
	alias_list = ['consistency']

	def __init__(self, config, datasource_name, on_change):
		DataChecker.__init__(self, config, datasource_name, on_change)
		self._mode = config.getEnum('%s check entry consistency' % datasource_name, DatasetCheckMode,
			DatasetCheckMode.abort, onChange = on_change)

	def enabled(self):
		return self._mode != DatasetCheckMode.ignore

	def process_block(self, block):
		# Check entry consistency
		events = sum(imap(lambda x: x[DataProvider.NEntries], block[DataProvider.FileList]))
		if block.setdefault(DataProvider.NEntries, events) != events:
			self._handle_error('Inconsistency in block %s: Number of events doesn\'t match (b:%d != f:%d)' % (
				DataProvider.bName(block), block[DataProvider.NEntries], events), self._mode)
		return block


class NickNameConsistencyProcessor(DataChecker):
	alias_list = ['nickconsistency']

	def __init__(self, config, datasource_name, on_change):
		DataChecker.__init__(self, config, datasource_name, on_change)
		# Ensure the same nickname is used consistently in all blocks of a dataset
		self._check_consistency = config.getEnum('%s check nickname consistency' % datasource_name, DatasetCheckMode,
			DatasetCheckMode.abort, onChange = on_change)
		self._check_consistency_data = {}
		# Check if two different datasets have the same nickname
		self._check_collision = config.getEnum('%s check nickname collision' % datasource_name, DatasetCheckMode,
			DatasetCheckMode.abort, onChange = on_change)
		self._check_collision_data = {}

	def enabled(self):
		return (self._check_consistency != DatasetCheckMode.ignore) or (self._check_collision != DatasetCheckMode.ignore)

	def process_block(self, block): # Get nickname and check for collisions
		dataset_name = block[DataProvider.Dataset]
		nick = block[DataProvider.Nickname]
		# Check if nickname is used consistenly in all blocks of a datasets
		if self._check_consistency != DatasetCheckMode.ignore:
			if self._check_consistency_data.setdefault(dataset_name, nick) != nick:
				self._handle_error('Different blocks of dataset "%s" have different nicknames: ["%s", "%s"]' % (
					dataset_name, self._check_consistency_data[dataset_name], nick), self._check_consistency)
		if self._check_collision != DatasetCheckMode.ignore:
			if self._check_collision_data.setdefault(nick, dataset_name) != dataset_name:
				self._handle_error('Multiple datasets use the same nickname "%s": ["%s", "%s"]' % (
					nick, self._check_collision_data[nick], dataset_name), self._check_collision)
		return block


class UniqueDataProcessor(DataChecker):
	alias_list = ['unique']

	def __init__(self, config, datasource_name, on_change):
		DataChecker.__init__(self, config, datasource_name, on_change)
		self._check_url = config.getEnum('%s check unique url' % datasource_name, DatasetUniqueMode, DatasetUniqueMode.abort, onChange = on_change)
		self._check_block = config.getEnum('%s check unique block' % datasource_name, DatasetUniqueMode, DatasetUniqueMode.abort, onChange = on_change)

	def enabled(self):
		return (self._check_url != DatasetUniqueMode.ignore) or (self._check_block != DatasetUniqueMode.ignore)

	def process(self, block_iter):
		self._recorded_url = set()
		self._recorded_block = set()
		return DataProcessor.process(self, block_iter)

	def process_block(self, block):
		# Check uniqueness of URLs
		url_hash_list = []
		if self._check_url != DatasetUniqueMode.ignore:
			def process_fi_list(fi_list):
				for fi in fi_list:
					url_hash = md5_hex(repr((fi[DataProvider.URL], fi[DataProvider.NEntries], fi.get(DataProvider.Metadata))))
					if url_hash in self._recorded_url:
						msg = 'Multiple occurences of URL: %r!' % fi[DataProvider.URL]
						msg += ' (This check can be configured with %r)' % 'dataset check unique url'
						if self._check_url == DatasetUniqueMode.warn:
							self._log.warning(msg)
						elif self._check_url == DatasetUniqueMode.abort:
							raise DatasetError(msg)
						elif self._check_url == DatasetUniqueMode.skip:
							continue
					self._recorded_url.add(url_hash)
					url_hash_list.append(url_hash)
					yield fi
			block[DataProvider.FileList] = list(process_fi_list(block[DataProvider.FileList]))
			url_hash_list.sort()

		# Check uniqueness of blocks
		if self._check_block != DatasetUniqueMode.ignore:
			block_hash = md5_hex(repr((block.get(DataProvider.Dataset), block[DataProvider.BlockName],
				url_hash_list, block[DataProvider.NEntries],
				block[DataProvider.Locations], block.get(DataProvider.Metadata))))
			if block_hash in self._recorded_block:
				msg = 'Multiple occurences of block: "%s"!' % DataProvider.bName(block)
				msg += ' (This check can be configured with %r)' % 'dataset check unique block'
				if self._check_block == DatasetUniqueMode.warn:
					self._log.warning(msg)
				elif self._check_block == DatasetUniqueMode.abort:
					raise DatasetError(msg)
				elif self._check_block == DatasetUniqueMode.skip:
					return None
			self._recorded_block.add(block_hash)
		return block
