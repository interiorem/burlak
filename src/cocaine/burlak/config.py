import os

import yaml

CONFIG_PATHS = [
    '/etc/cocaine/.cocaiane/tool.yml',
    '/etc/cocaine/orca.yml',
    '~/cocaine/orca.yml',
    '~/.cocaine/orca.yml',
]

DEFAULT_TOK_UPDATE_SEC = 10


#
# TODO: (cerberus) validator someday
#
# Should be compatible with tools secure section
#
class Config(object):

    def __init__(self):
        self._config = dict()

    def update(self, paths=CONFIG_PATHS):
        parsed = []
        for conf in paths:
            try:
                with open(os.path.expanduser(conf)) as fl:
                    print('Reading config from file {}', fl)
                    self.config.update(yaml.safe_load(fl.read()))
                    parsed.append(conf)
            except Exception as err:
                print('failed to read config file {}, {}'.format(conf, err))

        if not parsed:
            print('no config to read was found in file(s), using defaults.')

        return len(parsed)

    @property
    def config(self):
        return self._config

    @property
    def secure(self):
        secure_conf = self._config.get('secure', {})

        mod = secure_conf.get('mod', 'promisc')

        client_id = secure_conf.get('client_id', 0)
        client_secret = secure_conf.get('client_secret', '')

        tok_update = secure_conf.get('tok_update_sec', DEFAULT_TOK_UPDATE_SEC)

        return mod, client_id, client_secret, tok_update
