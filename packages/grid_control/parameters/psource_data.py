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

import os, time
from grid_control import utils
from grid_control.datasets import DataProvider, DatasetError
from grid_control.gc_exceptions import UserError
from grid_control.parameters.psource_base import LimitedResyncParameterSource
from grid_control.utils.activity import Activity
from python_compat import md5_hex, set


class DataParameterSource(LimitedResyncParameterSource):
	alias_list = ['data']

	def __init__(self, dn, ds_name, data_provider, data_splitter,
			data_proc, repository, keep_old=True):
		LimitedResyncParameterSource.__init__(self)
		(self._dn, self._name, self._part_proc, self._keep_old) = (dn, ds_name, data_proc, keep_old)
		(self._data_provider, self._data_splitter) = (data_provider, data_splitter)
		repository['dataset:%s' % ds_name] = self
		self.setup_resync(interval=-1)

		if not data_provider:  # debug mode - used by scripts - disables resync
			self._len = self._data_splitter.get_partition_len()
			return

		# look for aborted resyncs - and try to restore old state if possible
		if self._exists_data_path('cache.dat.resync') and self._exists_data_path('map.tar.resync'):
			utils.rename_file(self._get_data_path('cache.dat.resync'), self._get_data_path('cache.dat'))
			utils.rename_file(self._get_data_path('map.tar.resync'), self._get_data_path('map.tar'))
		elif self._exists_data_path('cache.dat.resync') or self._exists_data_path('map.tar.resync'):
			raise DatasetError('Found broken resync state')

		if self._exists_data_path('cache.dat') and self._exists_data_path('map.tar'):
			self._data_splitter.import_partitions(self._get_data_path('map.tar'))
		else:
			DataProvider.save_to_file(self._get_data_path('cache.dat'),
				self._data_provider.get_block_list_cached(show_stats=False))
			self._data_splitter.partition_blocks(self._get_data_path('map.tar'),
				self._data_provider.get_block_list_cached(show_stats=False))

		self._len = self._data_splitter.get_partition_len()

	def __repr__(self):
		return 'data(%s)' % utils.QM(self._name == 'data', '', self._name)

	def can_finish(self):
		return self._resync_interval < 0

	def create_psrc(cls, pconfig, repository, src='data'):  # pylint:disable=arguments-differ
		src_key = 'dataset:%s' % src
		if src_key not in repository:
			raise UserError('Dataset parameter source "%s" not setup!' % src)
		return repository[src_key]
	create_psrc = classmethod(create_psrc)

	def fill_parameter_content(self, pnum, result):
		partition = self._data_splitter.get_partition(pnum)
		self._part_proc.process(pnum, partition, result)

	def fill_parameter_metadata(self, result):
		result.extend(self._part_proc.get_partition_metadata() or [])

	def get_name(self):
		return self._name

	def get_needed_dataset_keys(self):
		return self._part_proc.get_needed_vn_list(self._data_splitter) or []

	def get_parameter_len(self):
		return self._len

	def show_psrc(self):
		return ['%s: src = %s' % (self.__class__.__name__, self._name)]

	def _exists_data_path(self, postfix):
		return os.path.exists(os.path.join(self._dn, self._name + postfix))

	def _get_data_path(self, postfix):
		return os.path.join(self._dn, self._name + postfix)

	def _get_psrc_hash(self):
		return md5_hex(repr([self._name, self._data_splitter.get_partition_len()]))

	def _resync_psrc(self):
		if self._data_provider:
			activity = Activity('Performing resync of datasource %r' % self._name)
			# Get old and new dataset information
			provider_old = DataProvider.load_from_file(self._get_data_path('cache.dat'))
			block_list_old = provider_old.get_block_list_cached(show_stats=False)
			self._data_provider.clear_cache()
			block_list_new = self._data_provider.get_block_list_cached(show_stats=False)
			self._data_provider.save_to_file(self._get_data_path('cache-new.dat'), block_list_new)

			# Use old splitting information to synchronize with new dataset infos
			partition_len_old = self._data_splitter.get_partition_len()
			partition_changes = self._data_splitter.resync_partitions(
				self._get_data_path('map-new.tar'), block_list_old, block_list_new)
			activity.finish()
			if partition_changes is not None:
				# Move current splitting to backup and use the new splitting from now on
				def _rename_with_backup(old, cur, new):
					if self._keep_old:
						os.rename(self._get_data_path(cur), self._get_data_path(old))
					os.rename(self._get_data_path(new), self._get_data_path(cur))
				_rename_with_backup('map-old-%d.tar' % time.time(), 'map.tar', 'map-new.tar')
				_rename_with_backup('cache-old-%d.dat' % time.time(), 'cache.dat', 'cache-new.dat')
				self._data_splitter.import_partitions(self._get_data_path('map.tar'))
				self._len = self._data_splitter.get_partition_len()
				self._log.debug('Dataset resync finished: %d -> %d partitions', partition_len_old, self._len)
				(pnum_list_redo, pnum_list_disable) = partition_changes
				return (set(pnum_list_redo), set(pnum_list_disable), partition_len_old != self._len)
