import time

from tornado import gen

from ..mixins import LoggerMixin


class MetricsFetcher(LoggerMixin):
    """Basic metrics provider

    Includes metrics:
        system - memory, cpu load, ect
        applications - TBD
    """
    def __init__(self, context, system_metrics, **kwargs):
        super(MetricsFetcher, self).__init__(context, **kwargs)

        self._system_metrics = system_metrics

    @gen.coroutine
    def fetch(self, _query, _type="tree"):
        """Try to mimic Cocaine metrics service API

        Note that API very likely subject of changes!
        """
        start = time.time()
        system = yield self._system_metrics.poll()
        self.debug('system metrics fetched in {:.3f}s', time.time() - start)

        raise gen.Return({
            'system': system,
            'applications': {},
        })
