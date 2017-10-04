# TODO: Tests.
#
import datetime


class VoidLogger(object):  # pragma nocover
    def debug(self, msg):
        pass

    def info(self, msg):
        pass

    def warn(self, msg):
        pass

    def error(self, msg):
        pass


class ConsoleLogger(VoidLogger):  # pragma nocover

    DEBUG, INFO, WARNING, ERROR = range(4)

    LEVELS = {
        DEBUG: 'DEBUG',
        INFO: 'INFO',
        WARNING: 'WARNING',
        ERROR: 'ERROR',
    }

    TFORMAT = '%Y-%m-%dT%H:%M:%S'

    def __init__(self, level=None, use_timestamp=True):
        self._level = ConsoleLogger.WARNING if level is None else level
        self._use_timestamp = use_timestamp

    def debug(self, msg):
        self._dump(ConsoleLogger.DEBUG, msg)

    def info(self, msg):
        self._dump(ConsoleLogger.INFO, msg)

    def warn(self, msg):
        self._dump(ConsoleLogger.WARN, msg)

    def error(self, msg):
        self._dump(ConsoleLogger.ERROR, msg)

    def _dump_current_ts(self):
        if self._use_timestamp:
            print datetime.datetime.utcnow().strftime(ConsoleLogger.TFORMAT),

    def _dump_msg(self, level, msg):
        print('{}: {}'.format(ConsoleLogger.LEVELS[level], msg))

    def _dump(self, level, msg):
        if self._level <= level:
            self._dump_current_ts()
            self._dump_msg(level, msg)

    @property
    def level(self):
        return self.level

    @level.setter
    def level(self, level):
        if level >= 0 and level < len(ConsoleLogger.LEVELS):
            self._level = level
        else:
            self._level = len(ConsoleLogger.LEVELS)
