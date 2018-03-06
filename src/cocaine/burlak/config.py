import os

import cerberus

import yaml

from .control_filter import ControlFilter
from .defaults import Defaults


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

    TASK_NAME = 'config'

    FILTER_SCHEMA = {
        'type': 'dict',
        'required': False,
        'schema': {
            'apply_control':  {
                'type': 'boolean',
                'required': False,
            },
            'white_list': {
                'type': 'list',
                'required': False,
                'schema': {
                    'type': 'string',
                },
            },
        },
    }

    # TODO: make schema work with tools config
    SCHEMA = {
        'locator': {
            'type': 'dict',
            'required': False,
            'schema': {
                'host': {'type': 'string'},
                'port': {
                    'type': 'integer',
                    'min': 0,
                    'max': 2**16
                }
            }
        },
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
        'stop_by_control': {
            'type': 'boolean',
            'required': False,
        },
        'control_with_ack': {
            'type': 'boolean',
            'required': False,
        },
        'pending_stop_in_state': {
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
        'console_log_level': {  # see ConsoleLogger.LEVELS for valie values
            'type': 'integer',
            'required': False,
        },
        'api_timeout_sec': {
            'type': 'integer',
            'min': 0,
            'max': 2**16,
            'required': False,
        },
        'status_web_path': {
            'type': 'string',
            'required': False,
        },
        'status_port': {
            'type': 'integer',
            'min': 0,
            'max': 2**16,
            'required': False,
        },
        'apps_poll_interval_sec': {
            'type': 'integer',
            'min': 0,
            'required': False,
        },
        'input_queue_size': {
            'type': 'integer',
            'min': 0,
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
        'control_filter_path': {
            'type': 'string',
            'required': False,
        },
        'control_filter': FILTER_SCHEMA,
    }

    def __init__(self, shared_status, logger=None):
        self._config = dict()
        self._validator = cerberus.Validator(self.SCHEMA, allow_unknown=True)
        self._logger = logger

        self._status = shared_status.register(Config.TASK_NAME)

    def _validate_raise(self, config):
        if not self._validator.validate(config):  # pragma nocover
            raise Exception('incorrect config format')

    def update(self, paths=CONFIG_PATHS):
        self._status.mark_ok('reading config')
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
            info_message = 'no config was found in file(s), using defaults.'
            self._info_to_logger(info_message, True)
            self._status.mark_ok(info_message)
        else:
            self._info_to_logger(
                'config has been updated from file(s) {}'.format(parsed),
                True)
            self._status.mark_ok(
                'config has been updated from {} file(s)'.format(len(parsed)))

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
        default_host, default_port = \
            Defaults.LOCATOR_HOST, Defaults.LOCATOR_PORT

        locator_section = self._config.get('locator')
        if locator_section and isinstance(locator_section, dict):
            default_host = locator_section.get('host', Defaults.LOCATOR_HOST)
            default_port = locator_section.get('port', Defaults.LOCATOR_PORT)

        return self._config.get(
            'locator_endpoints',
            [[default_host, default_port], ])

    @property
    def stop_apps(self):
        return self._config.get(
            'stop_apps',
            Defaults.STOP_APPS_NOT_IN_STATE)

    @property
    def stop_by_control(self):
        return self._config.get('stop_by_control', Defaults.STOP_BY_CONTROL)

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

    @property
    def apps_poll_interval_sec(self):
        return self._config.get(
            'apps_poll_interval_sec', Defaults.APPS_POLL_INTERVAL_SEC)

    @property
    def input_queue_size(self):
        return self._config.get(
            'input_queue_size', Defaults.INPUT_QUEUE_SIZE)

    @property
    def pending_stop_in_state(self):
        return self._config.get(
            'pending_stop_in_state', Defaults.PENDING_STOP_IN_STATE)

    @pending_stop_in_state.setter
    def pending_stop_in_state(self, flag):
        self._config['pending_stop_in_state'] = flag

    @console_log_level.setter
    def console_log_level(self, level):
        self._config['console_log_level'] = level

    @property
    def status_web_path(self):
        return self._config.get('status_web_path', Defaults.STATUS_WEB_PATH)

    @property
    def status_port(self):
        return self._config.get('status_port', Defaults.STATUS_PORT)

    @property
    def control_with_ack(self):
        return self._config.get('control_with_ack', Defaults.CONTROL_WITH_ACK)

    @property
    def control_filter_path(self):
        return self._config.get('control_filter_path', Defaults.FILTER_PATH)

    @property
    def control_filter(self):
        d = self._config.get('control_filter', dict())
        return ControlFilter.from_dict(d)

    @property
    def api_timeout(self):
        return self._config.get('api_timeout_sec', Defaults.API_TIMEOUT)

    @property
    def api_timeout_by2(self):
        return 2 * self._config.get('api_timeout_sec', Defaults.API_TIMEOUT)

    @control_filter.setter
    def control_filter(self, control_filter):
        self._config['control_filter'] = dict(
            apply_control=control_filter.apply_control,
            white_list=control_filter.white_list
        )

    # TODO:
    #   refactor to single method?
    #   make *args format
    def err_to_logger(self, msg, to_console=False):  # pragma nocover
        if self._logger:
            self._logger.error(msg)
        self._dump_to_console(msg, to_console)

    # TODO:
    #   refactor to single method?
    #   make *args format
    def _info_to_logger(self, msg, to_console=False):  # pragma nocover
        if self._logger:
            self._logger.info(msg)
        self._dump_to_console(msg, to_console)

    def _dump_to_console(self, msg, to_console=False):  # pragma nocovers
        if to_console:
            print (msg)
