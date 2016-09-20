#!/usr/bin/env python
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

import sys
from gcSupport import Options, Plugin, getConfig, scriptOptions, utils
from python_compat import lmap


parser = Options(usage = '%s [OPTIONS] <DBS dataset path>')
parser.add_text(None, '', 'producer', default = 'SimpleNickNameProducer', help = 'Name of the nickname producer')
options = scriptOptions(parser)

def main(opts, args):
	if not args:
		utils.exit_with_usage('Dataset path not specified!')
	datasetPath = args[0]
	if '*' in datasetPath:
		dbs3 = Plugin.create_instance('DBS3Provider', getConfig(), datasetPath, None)
		toProcess = dbs3.getCMSDatasetsImpl(datasetPath)
	else:
		toProcess = [datasetPath]

	nProd = Plugin.get_class('NickNameProducer').create_instance(opts.producer, getConfig())
	utils.display_table(
		[(0, 'Nickname'), (1, 'Dataset')],
		lmap(lambda ds: {0: nProd.getName('', ds, None), 1: ds}, toProcess), 'll')

sys.exit(main(options.opts, options.args))
