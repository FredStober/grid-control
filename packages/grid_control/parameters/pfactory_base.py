# | Copyright 2012-2016 Karlsruhe Institute of Technology
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

import random, logging
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.parameters.config_param import ParameterConfig
from grid_control.parameters.psource_base import ParameterError, ParameterSource
from grid_control.parameters.psource_lookup import parse_lookup_factory_args
from hpfwk import AbstractError
from python_compat import identity, ifilter, imap, irange, lfilter, lmap, sorted


class ParameterFactory(ConfigurablePlugin):
	config_tag_name = 'parameters'

	def get_source(self, repository):
		raise AbstractError


class BasicParameterFactory(ParameterFactory):
	def __init__(self, config):
		ParameterFactory.__init__(self, config)
		(self._psrc_list_const, self._psrc_list_lookup, self._psrc_list_nested) = ([], [], [])

		# Random number variables
		jobs_config = config.change_view(addSections=['jobs'])
		self._random_variables = jobs_config.get_list('random variables', ['JOB_RANDOM'], on_change=None)
		nseeds = jobs_config.get_int('nseeds', 10)
		seeds_new = lmap(lambda x: str(random.randint(0, 10000000)), irange(nseeds))
		self._random_seeds = jobs_config.get_list('seeds', seeds_new, persistent=True)

		# Get constants from [constants <tags...>]
		constants_config = config.change_view(view_class='TaggedConfigView',
			setClasses=None, setSections=['constants'], setNames=None)
		constants_pconfig = ParameterConfig(constants_config)
		for vn_const in ifilter(lambda opt: ' ' not in opt, constants_config.get_option_list()):
			constants_config.set('%s type' % vn_const, 'verbatim', '?=')
			self._register_psrc(constants_pconfig, vn_const.upper())

		param_config = config.change_view(view_class='TaggedConfigView',
			setClasses=None, addSections=['parameters'], inheritSections=True)

		# Get constants from [<Module>] constants
		task_pconfig = ParameterConfig(param_config)
		for vn_const in param_config.get_list('constants', []):
			config.set('%s type' % vn_const, 'verbatim', '?=')
			self._register_psrc(task_pconfig, vn_const)

		# Get global repeat value from 'parameters' section
		self._repeat = param_config.get_int('repeat', -1, on_change=None)
		self._req = param_config.get_bool('translate requirements', True, on_change=None)
		self._pfactory = param_config.get_plugin('parameter factory', 'SimpleParameterFactory',
			cls=ParameterFactory)

	def get_source(self, repository):
		source_list = []
		for name in self._random_variables:
			source_list.append(_create_psrc('RNGParameterSource', name))
		for (idx, seed) in enumerate(self._random_seeds):
			source_list.append(_create_psrc('CounterParameterSource', 'SEED_%d' % idx, int(seed)))
		source_list.extend(self._psrc_list_const)
		source_list.append(self._pfactory.get_source(repository))
		source_list.extend(self._psrc_list_lookup)
		source = _create_psrc('ZipLongParameterSource', *source_list)
		for (psrc_type, args) in self._psrc_list_nested:
			source = psrc_type(source, *args)
		if self._req:
			req_source = _create_psrc('RequirementParameterSource')
			source = _create_psrc('ZipLongParameterSource', source, req_source)
		source = self._use_available_data_psrc(source, repository)
		return _create_psrc('RepeatParameterSource', source, self._repeat)

	def _register_psrc(self, pconfig, vn):
		def replace_nonalnum(value):
			if str.isalnum(value):
				return value
			return ' '
		lookup_str = pconfig.get(vn, 'lookup', '')
		lookup_list = lfilter(identity, str.join('', imap(replace_nonalnum, lookup_str)).split())
		for (is_nested, psrc_type, args) in parse_lookup_factory_args(pconfig, [vn], lookup_list):
			if is_nested:  # switch needs elevation beyond local scope
				self._psrc_list_nested.append((psrc_type, args))
			else:
				psrc = psrc_type.create_instance(psrc_type.__name__, *args)
				if psrc.get_parameter_deps():
					self._psrc_list_lookup.append(psrc)
				else:
					self._psrc_list_const.append(psrc)

	def _use_available_data_psrc(self, source, repository):
		used_sources = source.get_used_psrc_list()
		unused_data_sources = []
		for (src_key, psrc_data) in sorted(repository.items()):
			if src_key.startswith('dataset:') and (psrc_data not in used_sources):
				unused_data_sources.append(psrc_data)
		if unused_data_sources:
			chained_data_sources = _create_psrc('ChainParameterSource', *unused_data_sources)
			source = _create_psrc('CrossParameterSource', chained_data_sources, source)
		return source


class UserParameterFactory(ParameterFactory):
	def __init__(self, config):
		ParameterFactory.__init__(self, config)
		self._log = logging.getLogger('parameters.factory')
		self._parameter_config = ParameterConfig(config)
		self._pexpr = config.get('parameters', '', on_change=None)

	def get_source(self, respository):
		if not self._pexpr:
			return _create_psrc('NullParameterSource')
		self._log.debug('Parsing parameter expression: %s', repr(self._pexpr))
		try:
			source = self._get_source_user(self._pexpr, respository)
		except:
			raise ParameterError('Unable to parse parameter expression %r' % self._pexpr)
		self._log.debug('Parsed parameter source: %s', repr(source))
		return source

	def _get_source_user(self, pexpr, respository):
		raise AbstractError


def _create_psrc(psrc_name, *args):
	return ParameterSource.create_instance(psrc_name, *args)
