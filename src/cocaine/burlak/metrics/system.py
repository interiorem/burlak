"""System matrics gather interface."""
from tornado import gen

from ..mixins import LoggerMixin
from .procfs import Cpu as ProcfsCPU
from .procfs import Loadavg as ProcfsLoadavg
from .procfs import Memory as ProcfsMemory
from .procfs import Network as ProcfsNetwork


class SystemMetrics(LoggerMixin):
    """Get system wide metrics.

    TODO: overlaps with sys_metrics.py code, should be merged someday.
    """

    def __init__(self, context, **kwargs):
        super(SystemMetrics, self).__init__(context, **kwargs)

        conf = context.config
        self._config = conf

        # System metrics
        self._cpu = ProcfsCPU(conf.procfs_stat_path)
        self._memory = ProcfsMemory(conf.procfs_mem_path)
        self._loadavg = ProcfsLoadavg(conf.procfs_loadavg_path)
        self._network = ProcfsNetwork(
            conf.procfs_netstat_path, conf.sysfs_network_prefix, conf.netlink)

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
         - network: mapping of network interfaces
        :rtype: dict[str, int | float | dict[str, int | float ]]

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

        try:
            network = yield self._network.read()
            network = ProcfsNetwork.as_named_dict(network)
            to_return.update({'network': network})
        except Exception as e:
            self.error('failed to get system metrics [network] {}', e)

        raise gen.Return(to_return)
