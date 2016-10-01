# | Copyright 2010-2016 Karlsruhe Institute of Technology
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

import os
from grid_control import utils
from grid_control.config import ConfigError
from grid_control.datasets import DataProvider
from grid_control.utils.parsing import str_dict_cfg
from grid_control_cms.cmssw import CMSSW
from grid_control_cms.lumi_tools import formatLumi, parseLumiFilter, strLumi
from python_compat import ichain, imap, lfilter, lmap, set, sorted


def formatLumiNice(lumis):
	lumi_filter_str = formatLumi(lumis)
	if len(lumi_filter_str) < 5:
		return str.join(', ', lumi_filter_str)
	return '%s ... %s (%d entries)' % (lumi_filter_str[0], lumi_filter_str[-1], len(lumi_filter_str))


class CMSSW_Advanced(CMSSW):
	config_section_list = CMSSW.config_section_list + ['CMSSW_Advanced']

	def __init__(self, config, name):
		self._name = name # needed for change_view calls before the constructor
		head = [('DATASETNICK', 'Nickname')]

		# Mapping between nickname and config files:
		self._nmCfg = config.get_lookup('nickname config', {}, default_matcher = 'regex',
			parser = lambda x: lmap(str.strip, x.split(',')), strfun = lambda x: str.join(',', x))
		if not self._nmCfg.empty():
			allConfigFiles = sorted(set(ichain(self._nmCfg.get_values())))
			config.set('config file', str.join('\n', allConfigFiles))
			head.append((1, 'Config file'))
		elif config.get('config file', ''):
			raise ConfigError("Please use 'nickname config' instead of 'config file'")

		# Mapping between nickname and constants - only display - work is handled by the 'normal' parameter factory
		nmCName = config.get_list('nickname constants', [], on_change = None)
		param_config = config.change_view(view_class = 'TaggedConfigView', setClasses = None, setNames = None, addSections = ['parameters'])
		param_config.set('constants', str.join(' ', nmCName), '+=')
		for cName in nmCName:
			param_config.set(cName + ' matcher', 'regex')
			param_config.set(cName + ' lookup', 'DATASETNICK')
			head.append((cName, cName))

		# Mapping between nickname and lumi filter - only display - work is handled by the 'normal' lumi filter
		config.set('lumi filter matcher', 'regex')
		if 'nickname lumi filter' in config.get_option_list():
			config.set('lumi filter', str_dict_cfg(config.get_dict('nickname lumi filter', {}, on_change = None)))
		self._nmLumi = config.get_lookup('lumi filter', {}, parser = parseLumiFilter, strfun = strLumi, on_change = None)
		if not self._nmLumi.empty():
			head.append((2, 'Lumi filter'))

		CMSSW.__init__(self, config, name)
		self._displaySetup(config.get_work_path('datacache.dat'), head)


	def _displaySetup(self, dsPath, head):
		if os.path.exists(dsPath):
			nickNames = set()
			for block in DataProvider.load_from_file(dsPath).get_block_list_cached(show_stats = False):
				nickNames.add(block[DataProvider.Nickname])
			self._log.info('Mapping between nickname and other settings:')
			report = []
			ps_lookup = lfilter(lambda ps: 'DATASETNICK' in ps.get_parameter_deps(), self.source.get_used_psrc_list())
			for nick in sorted(nickNames):
				tmp = {'DATASETNICK': nick}
				for src in ps_lookup:
					src.fill_parameter_content(None, tmp)
				tmp[1] = str.join(', ', imap(os.path.basename, self._nmCfg.lookup(nick, '', is_selector = False)))
				tmp[2] = formatLumiNice(self._nmLumi.lookup(nick, '', is_selector = False))
				report.append(tmp)
			utils.display_table(head, report, 'cl')


	def getTaskConfig(self):
		# Remove config file variable from the global settings
		data = CMSSW.getTaskConfig(self)
		data.pop('CMSSW_CONFIG')
		return data


	def getJobConfig(self, jobNum):
		data = CMSSW.getJobConfig(self, jobNum)
		config_file_list = self._nmCfg.lookup(data.get('DATASETNICK'), [], is_selector = False)
		data['CMSSW_CONFIG'] = str.join(' ', imap(os.path.basename, config_file_list))
		return data
