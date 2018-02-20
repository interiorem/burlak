from .logger import ConsoleLogger


class Defaults(object):
    '''App wide defaults
    '''
    TOK_UPDATE_SEC = 10

    WEB_PORT = 8877
    WEB_PATH = ''

    # TODO: make mandatory and config only
    UUID_PATH = '/darkvoice/states'

    NODE_SERVICE_NAME = 'node'
    UNICORN_SERVICE_NAME = 'unicorn'

    PROFILE_NAME = 'default'

    LOCATOR_HOST = 'localhost'
    LOCATOR_PORT = 10053

    STOP_APPS_NOT_IN_STATE = False
    PENDING_STOP_IN_STATE = False

    SENTRY_DSN = ''

    EXPIRE_STOPPED_SEC = 900

    # Default is skip all console logging.
    CONSOLE_LOGGER_LEVEL = int(ConsoleLogger.ERROR) + 1

    STATUS_WEB_PATH = r'/status'
    STATUS_PORT = 9878

    APPS_POLL_INTERVAL_SEC = 60
    INPUT_QUEUE_SIZE = 1024

    STOP_BY_CONTROL = False
    CONTROL_WITH_ACK = False

    APPLY_CONTROL = False
    FILTER_PATH = '/darkvoice/control_filter'
