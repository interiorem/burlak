from collections import namedtuple


LoggerSetup = namedtuple('LoggerSetup', [
    'logger',
    'dup_to_console',
])


class Context(object):
    '''Application wide internal state holders, handlers, e.g.:
    - config
    - logger
    '''
    def __init__(self, logger_setup, config):
        self._logger_setup = logger_setup
        self._config = config

    @property
    def logger_setup(self):
        return self._logger_setup

    @property
    def config(self):
        return self._config
