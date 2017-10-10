# TODO:
#  - Tests.
#  - Make API more user friendly: def warn(fmt, *args):
import datetime


class VoidLogger(object):  # pragma nocover
    def debug(self, fmt, *args):
        pass

    def info(self, fmt, *args):
        pass

    def warn(self, fmt, *args):
        pass

    def error(self, fmt, *args):
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

    def debug(self, fmt, *args):
        self._dump(ConsoleLogger.DEBUG, fmt, *args)

    def info(self, fmt, *args):
        self._dump(ConsoleLogger.INFO, fmt, *args)

    def warn(self, fmt, *args):
        self._dump(ConsoleLogger.WARNING, fmt, *args)

    def error(self, fmt, *args):
        self._dump(ConsoleLogger.ERROR, fmt, *args)

    def _dump_current_ts(self):
        if self._use_timestamp:
            print datetime.datetime.utcnow().strftime(ConsoleLogger.TFORMAT),

    def _dump_msg(self, level, fmt, *args):
        print(('{}: ' + fmt).format(ConsoleLogger.LEVELS[level], *args))

    def _dump(self, level, fmt, *args):
        if self._level <= level:
            self._dump_current_ts()
            self._dump_msg(level, fmt, *args)

    @property
    def level(self):
        return self.level

    @level.setter
    def level(self, level):
        if level >= 0 and level < len(ConsoleLogger.LEVELS):
            self._level = level
        else:
            self._level = len(ConsoleLogger.LEVELS)
