"""Metrics source abstraction."""
import time

from tornado import gen

from .system import SystemMetrics
from ..mixins import LoggerMixin


class MetricsSource(LoggerMixin):
    """Basic metrics provider.

    Includes metrics:
        system - memory, cpu load, ect
        applications - TBD

    Tries to mimic Cocaine Metrics service's API.
    """

    def __init__(self, context, hub, **kwargs):
        super(MetricsSource, self).__init__(context, **kwargs)

        self._system_metrics = SystemMetrics(context)
        self._hub = hub

    @gen.coroutine
    def fetch(self, _query, _type="tree"):
        """Try to mimic Cocaine metrics service API.

        Note that API very likely subject of change!
        """
        system = yield self._system_metrics.poll()
        self._hub.system = system

        raise gen.Return(self._hub.metrics)
