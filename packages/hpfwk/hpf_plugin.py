# | Copyright 2013-2016 Karlsruhe Institute of Technology
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

import os, sys, logging
from hpfwk.hpf_exceptions import ExceptionCollector, NestedException, clear_current_exception
from hpfwk.hpf_logging import init_hpf_logging


init_hpf_logging()  # needed for additional logging levels


class PluginError(NestedException):
	pass


def create_plugin_file(package, selector):
	cls_dict = {}

	def _fill_cls_dict(cls):
		# return list of dicts that were filled with cls information
		if cls == object:
			return [cls_dict]
		else:
			result = []
			for cls_base in cls.__bases__:
				for cls_base_dict in _fill_cls_dict(cls_base):
					tmp = cls_base_dict.setdefault(cls, {})
					tmp.setdefault(None, cls)
					result.append(tmp)
			return result

	for cls in get_plugin_list(import_modules(os.path.abspath(package), selector)):
		if cls.__module__.startswith(os.path.basename(package)):
			_fill_cls_dict(cls)

	def _write_cls_hierarchy(fp, data, level=0):
		if None in data:
			cls = data.pop(None)
			cls_name_str = str.join(' ', cls.get_class_name_list())
			fp.write('%s * %s %s\n' % (' ' * level, cls.__module__, cls_name_str))
			fp.write('\n')
		key_order = []
		for cls in data:
			key_order.append(tuple((cls.__module__ + '.' + cls.__name__).split('.')[::-1] + [cls]))
		key_order.sort()
		for key_info in key_order:
			_write_cls_hierarchy(fp, data[key_info[-1]], level + 1)

	if cls_dict:
		fp = open(os.path.abspath(os.path.join(package, '.PLUGINS')), 'w')
		try:
			_write_cls_hierarchy(fp, cls_dict)
		finally:
			fp.close()
		return cls_dict


def get_plugin_list(module_iterator):
	for module in module_iterator:
		try:
			cls_list = module.__all__
		except Exception:
			cls_list = dir(module)
		for cls_name in cls_list:
			cls = getattr(module, cls_name)
			try:
				if issubclass(cls, Plugin):
					yield cls
			except TypeError:
				clear_current_exception()


def import_modules(root, selector, package=None):
	sys.path = [os.path.abspath(root)] + sys.path

	if os.path.exists(os.path.join(root, '__init__.py')):
		package = (package or []) + [os.path.basename(root)]
		yield _safe_import(root, package)
	else:
		package = []

	fn_list = os.listdir(root)
	__import__('random').shuffle(fn_list)
	for fn in fn_list:
		if fn.endswith('.pyc'):
			os.remove(os.path.join(root, fn))
	for fn in fn_list:
		if not selector(os.path.join(root, fn)):
			continue
		if os.path.isdir(os.path.join(root, fn)):
			for module in import_modules(os.path.join(root, fn), selector, package):
				yield module
		elif os.path.isfile(os.path.join(root, fn)) and fn.endswith('.py'):
			yield _safe_import(root, package + [fn[:-3]])

	sys.path = sys.path[1:]


def init_hpf_plugins(base_dn):
	# Init plugin search paths
	plugin_fn = os.path.join(base_dn, '.PLUGINS')
	if os.path.exists(plugin_fn):
		__import__(os.path.basename(base_dn))  # Trigger initialisation of module
		map_level2cls_name = {}
		for line in open(plugin_fn):
			if not line.strip():
				continue
			tmp = line.split(' * ')
			module_info = tmp[1].split()
			(module_name, cls_name) = (module_info[0], module_info[1])
			cls_level = len(tmp[0])
			map_level2cls_name[cls_level] = cls_name
			for level in list(map_level2cls_name):
				if level > cls_level:
					map_level2cls_name.pop(level)
			level_cls_name_list = list(map_level2cls_name.items())
			level_cls_name_list.sort()
			base_cls_list = []
			for (_, base_cls_name) in level_cls_name_list:
				base_cls_list.append(base_cls_name)
			Plugin.register_class(module_name, cls_name,
				alias_list=module_info[2:], base_cls_names=base_cls_list)


class InstanceFactory(object):
	# Wrapper class to fix plugin arguments
	def __init__(self, get_bind_value, cls, *args, **kwargs):
		(self._bind_value, self._cls, self._args, self._kwargs) = (get_bind_value, cls, args, kwargs)

	def __eq__(self, other):  # Used to check for changes compared to old
		return self.get_bind_value() == other.get_bind_value()

	def __repr__(self):
		return '<instance factory for %s>' % self._format_call(
			self._args, self._kwargs, add_ellipsis=True)

	def create_instance_bound(self, *args, **kwargs):
		args = self._args + args
		kwargs = dict(list(self._kwargs.items()) + list(kwargs.items()))
		try:
			return self._cls(*args, **kwargs)
		except Exception:
			raise PluginError('Error while creating instance: %s' % self._format_call(args, kwargs))

	def get_bind_value(self):
		return self._bind_value

	def _format_call(self, args, kwargs, add_ellipsis=False):
		cls_name = '%s.%s' % (self._cls.__module__, self._cls.__name__)
		if not logging.getLogger().isEnabledFor(logging.INFO1):
			return repr(cls_name)
		args_str_list = []
		for arg in args:
			args_str_list.append(repr(arg))
		for k_v in kwargs.items():
			args_str_list.append('%s=%r' % k_v)
		if add_ellipsis:
			args_str_list.append('...')
		return cls_name + '(%s)' % str.join(', ', args_str_list)


class Plugin(object):
	# Abstract class taking care of dynamic class loading
	alias_list = []
	config_section_list = []

	_cls_cache = {}
	_map_cls_alias2cls_base_list = {}
	_map_cls_alias2depth_fqname = {}
	_map_cls_name2child_info_list = {}

	def bind(cls, value, **kwargs):
		for entry in value.split():
			yield InstanceFactory(entry, cls.get_class(entry))
	bind = classmethod(bind)

	def create_instance(cls, cls_name, *args, **kwargs):
		# Get an instance of a derived class by specifying the class name and constructor arguments
		factory = InstanceFactory(cls_name, cls.get_class(cls_name), *args, **kwargs)
		return factory.create_instance_bound()  # For uniform error output
	create_instance = classmethod(create_instance)

	def get_class(cls, cls_name):
		log = logging.getLogger('classloader.%s' % cls.__name__.lower())
		log.log(logging.DEBUG2, 'Loading class %s', cls_name)
		if cls_name not in cls._cls_cache.get(cls, {}):
			for result in cls._get_class_checked(log, cls_name):
				cls._cls_cache.setdefault(cls, {})[cls_name] = result
				break  # return only first class
		return cls._cls_cache[cls][cls_name]
	get_class = classmethod(get_class)

	def get_class_info_list(cls):
		return Plugin._map_cls_name2child_info_list.get(cls.__name__.lower(), [])
	get_class_info_list = classmethod(get_class_info_list)

	def get_class_name_list(cls):
		for parent_cls in cls.__bases__:
			if hasattr(parent_cls, 'alias_list') and (cls.alias_list == parent_cls.alias_list):
				return [cls.__name__]  # class aliases are not inherited
		return [cls.__name__] + cls.alias_list
	get_class_name_list = classmethod(get_class_name_list)

	def iter_class_bases(cls, add_current_cls=True):
		if add_current_cls:
			yield cls
		for parent_cls in cls.__bases__:
			if issubclass(parent_cls, Plugin):
				for entry in parent_cls.iter_class_bases():
					yield entry
	iter_class_bases = classmethod(iter_class_bases)

	def register_class(cls, module_name, cls_name, alias_list, base_cls_names):
		cls_path = '%s.%s' % (module_name, cls_name)
		cls_depth = len(base_cls_names)
		for name in [cls_name] + alias_list:
			cls_name_entry = cls._map_cls_alias2depth_fqname.setdefault(name.lower(), [])
			cls._map_cls_alias2cls_base_list[name.lower()] = base_cls_names
			if cls_path not in cls_name_entry:
				if name != cls_name:
					cls_depth = len(base_cls_names) + 1
				cls_name_entry.append((-cls_depth, cls_path))
			for base_cls_name in base_cls_names:
				cls_base_entry = cls._map_cls_name2child_info_list.setdefault(base_cls_name.lower(), [])
				tmp = {name: cls_name, 'depth': cls_depth}
				if tmp not in cls_base_entry:
					cls_base_entry.append(tmp)
	register_class = classmethod(register_class)

	def _get_class(cls, exc, log, cls_name, cls_processed, cls_bad_parents):
		# resolve class name/alias to complete class path 'myplugin -> module.submodule.MyPlugin'
		cls_search_list = [(0, cls_name)]
		while cls_search_list:
			_, cls_search_name = cls_search_list.pop()
			if cls_search_name in cls_processed:  # Prevent lookup circles
				continue
			cls_processed.append(cls_search_name)
			cls_module_list = []
			if '.' in cls_search_name:  # module.submodule.class specification
				cls_module_list.extend(_get_module_list(exc, log, cls_search_name))
				cls_search_name = cls_search_name.split('.')[-1]
			elif hasattr(sys.modules['__main__'], cls_search_name):
				cls_module_list.append(sys.modules['__main__'])

			cls_iter = _get_class_list_from_modules(exc, log, cls_search_name, cls_module_list)
			for result in _filter_class_with_parent(exc, log, cls_iter, cls, cls_bad_parents):
				yield result
			cls_search_list.extend(cls._map_cls_alias2depth_fqname.get(cls_search_name.lower(), []))
			cls_search_list.sort()  # sort by class inheritance depth
	_get_class = classmethod(_get_class)

	def _get_class_checked(cls, log, cls_name):
		cls_list_found = []
		cls_list_processed = []
		cls_list_bad_parents = []
		exc = ExceptionCollector(log)
		for result in cls._get_class(exc, log, cls_name, cls_list_processed, cls_list_bad_parents):
			if result not in cls_list_found:
				cls_list_found.append(result)
				yield result
		if not cls_list_found:
			msg = 'Unable to load %r of type %r\n' % (cls_name, _get_fq_class_name(cls))
			if cls_list_processed:
				msg += '\tsearched plugin names:\n\t\t%s\n' % str.join('\n\t\t', cls_list_processed)
			if cls_list_bad_parents:
				msg += '\tfound incompatible plugins:\n\t\t%s\n' % str.join('\n\t\t', cls_list_bad_parents)
			exc.raise_any(PluginError(msg))
			raise PluginError(msg)
	_get_class_checked = classmethod(_get_class_checked)


def _filter_class_with_parent(exc, log, cls_iter, cls_parent, cls_list_bad_parents):
	for cls in cls_iter:
		try:
			if issubclass(cls, cls_parent):
				log.log(logging.DEBUG, 'Successfully loaded class %s', _get_fq_class_name(cls))
				yield cls
			cls_list_bad_parents.append(cls.__name__)
			log.log(logging.DEBUG1, '%s is not of type %s!',
				_get_fq_class_name(cls), _get_fq_class_name(cls_parent))
		except Exception:
			exc.collect()


def _get_class_list_from_modules(exc, log, cls_name, cls_module_list):
	for cls_module in cls_module_list:
		log.log(logging.DEBUG3, 'Searching for class %s:%s', cls_module.__name__, cls_name)
		try:
			yield getattr(cls_module, cls_name)
		except Exception:
			exc.collect(logging.DEBUG3, 'Unable to import class %s:%s', cls_module.__name__, cls_name)


def _get_fq_class_name(cls):
	return '%s:%s' % (cls.__module__, cls.__name__)


def _get_module_list(exc, log, cls_name):
	cls_name_parts = cls_name.split('.')
	cls_name = cls_name_parts[-1]
	cls_module_name = str.join('.', cls_name_parts[:-1])
	log.log(logging.DEBUG3, 'Importing module %s', cls_module_name)
	old_sys_path = list(sys.path)
	result = []
	try:
		result = [__import__(cls_module_name, {}, {}, [cls_name])]
	except Exception:
		exc.collect(logging.DEBUG3, 'Unable to import module %s', cls_module_name)
	sys.path = old_sys_path
	return result


def _safe_import(root, module):
	old_path = list(sys.path)
	try:
		result = __import__(str.join('.', module), {}, {}, module[-1])
	except Exception:
		sys.stderr.write('import error: %s %s\n%r' % (root, module, sys.path))
		raise
	sys.path = old_path
	return result
