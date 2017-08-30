import os

import cerberus
import yaml


CONFIG_PATHS = [
    '/etc/cocaine/.cocaiane/tool.yml',
    '/etc/cocaine/orca.yaml',
    '~/cocaine/orca.yaml',
    '~/.cocaine/orca.yaml',
]


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

    DEFAULT_LOCATOR_HOST = 'localhost'
    DEFAULT_LOCATOR_PORT = 10053

    SCHEMA = {
        'secure': {
            'type': 'dict',
            'required': False,
            'schema': {
                'mod': {
                    'type': 'string',
                    'allowed': [
                        'tvm',
                        'promiscuous',
                        'test1', 'test2'
                    ]
                },
                'client_id': {'type': 'integer'},
                'client_secret': {'type': 'string'},
                'tok_update': {'type': 'integer'},
            },
        },
        'unicorn_service_name': {
            'type': 'string',
            'required': False,
        },
        'node_service_name': {
            'type': 'string',
            'required': False,
        },
        'port': {
            'type': 'integer',
            'min': 0,
            'required': False,
        },
        'web_path': {
            'type': 'string',
            'required': False,
        },
        'uuid_path': {
            'type': 'string',
            'required': False,
        },
        'default_profile': {
            'type': 'string',
            'required': False,
        },
        'locator_endpoints': {
            'type': 'list',
            'required': False,
            'schema': {
                'type': 'list',
                'items': [
                    {'type': 'string'},   # host
                    {'type': 'integer'},  # port
                ],
            },
        },
    }

    def __init__(self):
        self._config = dict()
        self._validator = cerberus.Validator(self.SCHEMA, allow_unknown=True)

    def _validate_raise(self, config):
        if not self._validator.validate(config):  # pragma nocover
            raise Exception('incorrect config format')

    def update(self, paths=CONFIG_PATHS):
        parsed = []
        for conf in paths:
            try:
                with open(os.path.expanduser(conf)) as fl:
                    print('Reading config from file {}'.format(conf))

                    config = yaml.safe_load(fl.read())
                    if config:
                        self._validate_raise(config)
                        self._config.update(config)

                    parsed.append(conf)
            except Exception as err:
                print('failed to read config file {}, {}'.format(conf, err))

        if not parsed:  # pragma nocover
            print('no config to read was found in file(s), using defaults.')

        return len(parsed)

    @property
    def config(self):  # pragma nocover
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
    def web_endpoint(self):
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

    @property
    def locator_endpoints(self):
        return self._config.get(
            'locator_endpoints',
            [[Config.DEFAULT_LOCATOR_HOST, Config.DEFAULT_LOCATOR_PORT], ])
