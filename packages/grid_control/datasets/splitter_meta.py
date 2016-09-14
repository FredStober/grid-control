# | Copyright 2011-2016 Karlsruhe Institute of Technology
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

from grid_control.datasets.provider_base import DataProvider
from grid_control.datasets.splitter_basic import FileLevelSplitter
from hpfwk import AbstractError
from python_compat import imap, sort_inplace

# Split dataset along block and metadata boundaries - using equivalence classes of metadata
class MetadataSplitter(FileLevelSplitter):
	def _get_metadata_key(self, metadataNames, block, fi):
		raise AbstractError

	def _proto_partition_blocks(self, blocks):
		for block in blocks:
			files = block[DataProvider.FileList]
			sort_inplace(files, key = lambda fi: self._get_metadata_key(block.get(DataProvider.Metadata, []), block, fi))
			(fileStack, reprKey) = ([], None)
			for fi in files:
				if reprKey is None:
					reprKey = self._get_metadata_key(block[DataProvider.Metadata], block, fi)
				curKey = self._get_metadata_key(block[DataProvider.Metadata], block, fi)
				if curKey != reprKey:
					yield self._create_partition(block, fileStack)
					(fileStack, reprKey) = ([], curKey)
				fileStack.append(fi)
			yield self._create_partition(block, fileStack)


class UserMetadataSplitter(MetadataSplitter):
	alias = ['metadata']

	def _configure_splitter(self, config):
		self._metadata = self._query_config(config.getList, 'split metadata', [])

	def _get_metadata_key(self, metadataNames, block, fi):
		selMetadataNames = self._setup(self._metadata, block)
		selMetadataIdx = []
		for selMetadataName in selMetadataNames:
			if selMetadataName in metadataNames:
				selMetadataIdx.append(metadataNames.index(selMetadataName))
			else:
				selMetadataIdx.append(-1)
		def query_metadata(idx):
			if (idx >= 0) and (idx < len(fi[DataProvider.Metadata])):
				return fi[DataProvider.Metadata][idx]
			return ''
		return tuple(imap(query_metadata, selMetadataIdx))
