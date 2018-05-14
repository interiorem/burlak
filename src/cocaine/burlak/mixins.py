
from collections import defaultdict
from .logger import ConsoleLogger, VoidLogger


SELF_NAME = 'app/orca'  # aka 'Killer Whale'


class MetricsMixin(object):
    def __init__(self, **kwargs):
        super(MetricsMixin, self).__init__(**kwargs)
        self.metrics_cnt = defaultdict(int)

    def get_count_metrics(self):
        return self.metrics_cnt


class LoggerMixin(object):  # pragma nocover
    def __init__(self, context, name=SELF_NAME, **kwargs):
        super(LoggerMixin, self).__init__(**kwargs)

        self.logger = context.logger_setup.logger
        self.format = '{} :: %s'.format(name)
        self.console = ConsoleLogger(context.config.console_log_level) \
            if context.logger_setup.dup_to_console \
            else VoidLogger()

    def debug(self, fmt, *args):
        self.console.debug(fmt, *args)
        self.logger.debug(self.format, fmt.format(*args))

    def info(self, fmt, *args):
        self.console.info(fmt, *args)
        self.logger.info(self.format, fmt.format(*args))

    def warn(self, fmt, *args):
        self.console.warn(fmt, *args)
        self.logger.warn(self.format, fmt.format(*args))

    def error(self, fmt, *args):
        self.console.error(fmt, *args)
        self.logger.error(self.format, fmt.format(*args))
