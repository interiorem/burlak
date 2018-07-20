"""Metrics source abstraction."""
import time

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

    def fetch(self, _query, _type="tree"):
        """Try to mimic Cocaine metrics service API.

        Note that API very likely subject of change!
        """
        system = self._system_metrics.poll()
        self._hub.system = system

        return self._hub.metrics
