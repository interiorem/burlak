from tornado import gen

from ..mixins import LoggerMixin
from .procfs import Cpu as ProcfsCPU
from .procfs import Loadavg as ProcfsLoadavg
from .procfs import Memory as ProcfsMemory


class SystemMetrics(LoggerMixin):
    """Gets system wide metrics

    TODO: overlaps with sys_metrics.py code, should be merged someday.
    """
    def __init__(self, context, **kwargs):
        super(SystemMetrics, self).__init__(context, **kwargs)

        self._config = context.config

        # System metrics
        self._cpu = ProcfsCPU(context.config.procfs_stat_name)
        self._memory = ProcfsMemory(context.config.procfs_mem_name)
        self._loadavg = ProcfsLoadavg(context.config.procfs_loadavg_name)

    @gen.coroutine
    def poll(self):
        """Poll procfs metrics.

        :return: system (host's procfs) metrics
        Implemented:
         - cpu.load: total CPUs load expressed as ratio in interval [0, 1]
         - cpu.usable: total CPUs idle expressed as ratio in interval [0, 1]
         - mem.load: memory load as ratio in interval [0, 1]
         - mem.total
         - mem.free
         - mem.used
        :rtype: dict[str, numeric]

        TODO(SystemMetrics): seems that try/exept blocks is code repititions
        """
        to_return = {}

        try:
            cpu = yield self._cpu.read()
            to_return.update({
                'cpu.load': cpu.load,
                'cpu.usable': cpu.usable,
            })
        except Exception as e:
            self.error('failed to get system metrics [cpu] {}', e)

        try:
            memory = yield self._memory.read()
            to_return.update({
                'mem.load': memory.load,
                'mem.cached': memory.cached,
                'mem.free': memory.free,
                'mem.used': memory.used,
                'mem.total': memory.total,
            })
        except Exception as e:
            self.error('failed to get system metrics [memory] {}', e)

        try:
            loadavg = yield self._loadavg.read()
            to_return.update({'loadavg': loadavg})
        except Exception as e:
            self.error('failed to get system metrics [loadavg] {}', e)

        raise gen.Return(to_return)
