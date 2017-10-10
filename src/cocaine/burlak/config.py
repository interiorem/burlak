import os

import cerberus
import yaml

from .logger import ConsoleLogger

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


class Defaults(object):
    TOK_UPDATE_SEC = 10

    WEB_PORT = 8877
    WEB_PATH = ''

    # TODO: make mandatory and config only
    # UUID_PATH = '/state'
    UUID_PATH = '/darkvoice/states'

    NODE_SERVICE_NAME = 'node'
    UNICORN_SERVICE_NAME = 'unicorn'

    PROFILE_NAME = 'default'

    LOCATOR_HOST = 'localhost'
    LOCATOR_PORT = 10053

    STOP_APPS_NOT_IN_STATE = False

    SENTRY_DSN = ''

    EXPIRE_STOPPED_SEC = 600
    EXPIRE_CACHED_APP_SEC = 3600

    # Default is skip all console logging.
    CONSOLE_LOGGER_LEVEL = int(ConsoleLogger.ERROR) + 1


#
# Should be compatible with tools secure section
#
class Config(object):

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
        # TODO: add `_sec` suffix and make app-wide update
        'expire_stopped': {
            'type': 'integer',
            'required': False,
        },
        'expire_cached_app_sec': {
            'type': 'integer',
            'required': False,
        },
        'console_log_level': {  # see ConsoleLogger.LEVELS for valie values
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
            Defaults.TOK_UPDATE_SEC)

        return mod, client_id, client_secret, tok_update

    @property
    def web_endpoint(self):
        port = self._config.get('port', Defaults.WEB_PORT)
        path = self._config.get('web_path', Defaults.WEB_PATH)

        return port, path

    @property
    def uuid_path(self):
        return self._config.get('uuid_path', Defaults.UUID_PATH)

    @property
    def node_name(self):
        return self._config.get(
            'node_service_name', Defaults.NODE_SERVICE_NAME)

    @property
    def unicorn_name(self):
        return self._config.get(
            'unicorn_service_name', Defaults.UNICORN_SERVICE_NAME)

    @property
    def default_profile(self):
        return self._config.get(
            'default_profile', Defaults.PROFILE_NAME)

    @property
    def locator_endpoints(self):
        return self._config.get(
            'locator_endpoints',
            [[Defaults.LOCATOR_HOST, Defaults.LOCATOR_PORT], ])

    @property
    def stop_apps(self):
        return self._config.get(
            'stop_apps',
            Defaults.STOP_APPS_NOT_IN_STATE)

    @property
    def sentry_dsn(self):
        return self._config.get('sentry_dsn', Defaults.SENTRY_DSN)

    @property
    def expire_stopped(self):
        return self._config.get(
            'expire_stopped', Defaults.EXPIRE_STOPPED_SEC)

    @property
    def console_log_level(self):
        return self._config.get(
            'console_log_level', Defaults.CONSOLE_LOGGER_LEVEL)

    @console_log_level.setter
    def console_log_level(self, level):
        self._config['console_log_level'] = level

    @property
    def expire_cached_app_sec(self):
        return self._config.get(
            'expire_cached_app_sec', Defaults.EXPIRE_CACHED_APP_SEC)

    # TODO:
    #   refactor to single method?
    #   make *args format
    def err_to_logger(self, msg, to_console=False):  # pragma nocover
        if self._logger:
            self._logger.error(msg)
        self.dump_to_console(msg, to_console)

    # TODO:
    #   refactor to single method?
    #   make *args format
    def info_to_logger(self, msg, to_console=False):  # pragma nocover
        if self._logger:
            self._logger.info(msg)
        self.dump_to_console(msg, to_console)

    def dump_to_console(self, msg, to_console=False):  # pragma nocovers
        if to_console:
            print (msg)
