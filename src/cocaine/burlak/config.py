import os

import cerberus
import yaml


CONFIG_PATHS = [
    '/etc/cocaine/.cocaine/tools.yml',
    '/etc/cocaine/.cocaine/tools.yaml',
    '/etc/cocaine/orca.yaml',
    # `orca.dynamic.yaml` is used for template dynamic config per:
    # DC, cluster, ect.
    '/etc/cocaine/orca.dynamic.yaml',  # for dynamic sentry config only!
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

    # TODO: make mandatory and config only
    # DEFAULT_UUID_PATH = '/state'
    DEFAULT_UUID_PATH = '/darkvoice/states'

    DEFAULT_NODE_SERVICE_NAME = 'node'
    DEFAULT_UNICORN_SERVICE_NAME = 'unicorn'

    DEFAULT_PROFILE_NAME = 'default'

    DEFAULT_LOCATOR_HOST = 'localhost'
    DEFAULT_LOCATOR_PORT = 10053

    DEFAULT_STOP_APPS_NOT_IN_STATE = False

    DEFAULT_SENTRY_DSN = ''

    DEFAULT_EXPIRE_STOPPED_SEC = 600

    # TODO: make schema work with tools config
    SCHEMA = {
        'secure': {
            'type': 'dict',
            'required': False,
            'schema': {
                'mod': {
                    'type': 'string',
                    'allowed': [
                        'tvm',
                        'TVM',
                        'promiscuous',
                        'test1', 'test2'
                    ]
                },
                'client_id': {'type': 'integer'},
                'client_secret': {'type': 'string'},
                'tok_update': {'type': 'integer', 'required': False},
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
        'stop_apps': {
            'type': 'boolean',
            'required': False,
        },
        'sentry_dsn': {
            'type': 'string',
            'required': False,
        },
        'expire_stopped': {
            'type': 'integer',
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

    def __init__(self, logger=None):
        self._config = dict()
        self._validator = cerberus.Validator(self.SCHEMA, allow_unknown=True)
        self._logger = logger

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
                        # TODO: temporary disabled
                        self._validate_raise(config)
                        self._config.update(config)

                    parsed.append(conf)
            except Exception as err:
                self.err_to_logger(
                    'failed to read config file {}, {}'.format(conf, err),
                    True)

        if not parsed:  # pragma nocover
            self.info_to_logger(
                'no config was found in file(s), using defaults.',
                True)
        else:
            self.info_to_logger(
                'config has been updated from file(s) {}'.format(parsed),
                True)

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

    @property
    def stop_apps(self):
        return self._config.get(
            'stop_apps',
            Config.DEFAULT_STOP_APPS_NOT_IN_STATE)

    @property
    def sentry_dsn(self):
        return self._config.get('sentry_dsn', Config.DEFAULT_SENTRY_DSN)

    @property
    def expire_stopped(self):
        return self._config.get(
            'expire_stopped', Config.DEFAULT_EXPIRE_STOPPED_SEC)

    # TODO: refactor to single method?
    def err_to_logger(self, msg, to_console=False):  # pragma nocover
        if self._logger:
            self._logger.error(msg)
        self.dump_to_console(msg, to_console)

    # TODO: refactor to single method?
    def info_to_logger(self, msg, to_console=False):  # pragma nocover
        if self._logger:
            self._logger.info(msg)
        self.dump_to_console(msg, to_console)

    def dump_to_console(self, msg, to_console=False):  # pragma nocovers
        if to_console:
            print (msg)
