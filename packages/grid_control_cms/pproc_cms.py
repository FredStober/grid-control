# | Copyright 2017 Karlsruhe Institute of Technology
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

from grid_control.datasets import DataSplitter, PartitionProcessor
from grid_control.parameters import ParameterMetadata
from python_compat import imap, lmap, reduce, set, sorted


class LFNPartitionProcessor(PartitionProcessor):
	alias_list = ['lfnprefix']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		lfn_modifier = config.get(self._get_pproc_opt('lfn modifier'), '')
		lfn_modifier_shortcuts = config.get_dict(self._get_pproc_opt('lfn modifier dict'), {
			'<xrootd>': 'root://cms-xrd-global.cern.ch/',
			'<xrootd:eu>': 'root://xrootd-cms.infn.it/',
			'<xrootd:us>': 'root://cmsxrootd.fnal.gov/',
		})[0]
		self._prefix = None
		if lfn_modifier == '/':
			self._prefix = '/store/'
		elif lfn_modifier.lower() in lfn_modifier_shortcuts:
			self._prefix = lfn_modifier_shortcuts[lfn_modifier.lower()] + '/store/'
		elif lfn_modifier:
			self._prefix = lfn_modifier + '/store/'

	def enabled(self):
		return self._prefix is not None

	def get_partition_metadata(self):
		return lmap(lambda k: ParameterMetadata(k, untracked=True), ['DATASET_SRM_FILES'])

	def process(self, pnum, partition, result):
		def _modify_filelist_for_srm(filelist):
			return lmap(lambda f: 'file://' + f.split('/')[-1], filelist)

		def _prefix_lfn(lfn):
			return self._prefix + lfn.split('/store/', 1)[-1]

		if self._prefix:
			partition[DataSplitter.FileList] = lmap(_prefix_lfn, partition[DataSplitter.FileList])
			if 'srm' in self._prefix:
				result.update({'DATASET_SRM_FILES': str.join(' ', partition[DataSplitter.FileList])})
				partition[DataSplitter.FileList] = _modify_filelist_for_srm(partition[DataSplitter.FileList])


BasicPartitionProcessor = PartitionProcessor.get_class('BasicPartitionProcessor')  # pylint:disable=invalid-name


class CMSSWPartitionProcessor(BasicPartitionProcessor):
	alias_list = ['cmsswpart']

	def __init__(self, config, datasource_name):
		BasicPartitionProcessor.__init__(self, config, datasource_name)
		self._vn_secondary_file_names = config.get(
			self._get_pproc_opt('variable secondary file names'), 'FILE_NAMES2')
		self._meta_secondary_file_names = config.get(
			self._get_pproc_opt('metadata secondary file names'), 'CMSSW_PARENT_LFNS')

	def get_partition_metadata(self):
		result = BasicPartitionProcessor.get_partition_metadata(self)
		result.append(ParameterMetadata(self._vn_secondary_file_names, untracked=True))
		return result

	def process(self, pnum, partition_info, result):
		BasicPartitionProcessor.process(self, pnum, partition_info, result)
		metadata_header = partition_info.get(DataSplitter.MetadataHeader, [])
		if self._meta_secondary_file_names in metadata_header:
			parent_lfn_info_idx = metadata_header.index(self._meta_secondary_file_names)
			parent_lfn_list_list = partition_info[DataSplitter.Metadata][parent_lfn_info_idx]
			parent_lfn_list = sorted(set(reduce(list.__add__, parent_lfn_list_list, [])))
			result[self._vn_secondary_file_names] = self._format_fn_list(parent_lfn_list)

	def _format_fn_list(self, fn_list):
		return str.join(', ', imap(lambda x: '"%s"' % x, fn_list))
