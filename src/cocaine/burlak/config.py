import os

import yaml

CONFIG_PATHS = [
    '/etc/cocaine/.cocaiane/tool.yml',
    '/etc/cocaine/orca.yml',
    '~/cocaine/orca.yml',
    '~/.cocaine/orca.yml',
]


#
# TODO: (cerberus) validator someday
#
# Should be compatible with tools secure section
#
class Config(object):

    DEFAULT_TOK_UPDATE_SEC = 10

    DEFAULT_WEB_PORT = 8877
    DEFAULT_WEB_PATH = ''

    DEFAULT_UUID_PATH = '/state'

    DEFAULT_NODE_SERVICE_NAME = 'node'
    DEFAULT_UNICORN_SERVICE_NAME = 'unicorn'

    DEFAULT_PROFILE_NAME = 'default'

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
        tok_update = secure_conf.get(
            'tok_update_sec',
            self.DEFAULT_TOK_UPDATE_SEC)

        return mod, client_id, client_secret, tok_update

    @property
    def endpoint(self):
        port = self._config.get('port', self.DEFAULT_WEB_PORT)
        path = self._config.get('web_path', self.DEFAULT_WEB_PATH)

        return port, path

    @property
    def uuid_path(self):
        return self._config.get('uuid_path', self.DEFAULT_UUID_PATH)

    @property
    def node_name(self):
        return self._config.get(
            'node_service_name', self.DEFAULT_NODE_SERVICE_NAME)

    @property
    def unicorn_name(self):
        return self._config.get(
            'unicorn_service_name', self.DEFAULT_UNICORN_SERVICE_NAME)

    @property
    def default_profile(self):
        return self._config.get(
            'default_profile', self.DEFAULT_PROFILE_NAME)
