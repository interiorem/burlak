"""System matrics gather interface."""

from ..mixins import LoggerMixin
from .procfs import Cpu as ProcfsCPU
from .procfs import Loadavg as ProcfsLoadavg
from .procfs import Memory as ProcfsMemory
from .procfs import Network as ProcfsNetwork
from .ewma import EWMA


class SystemMetrics(LoggerMixin):
    """Get system wide metrics.

    TODO: overlaps with sys_metrics.py code, should be merged someday.
    """

    def __init__(self, context, **kwargs):
        super(SystemMetrics, self).__init__(context, **kwargs)

        config = self._config = context.config
        metrics_config = config.metrics

        alpha = EWMA.alpha(
            metrics_config.poll_interval_sec,
            metrics_config.post_interval_sec,
        )

        # System metrics
        self._cpu = ProcfsCPU(config.procfs_stat_path, alpha)
        self._memory = ProcfsMemory(config.procfs_mem_path, alpha)
        self._loadavg = ProcfsLoadavg(config.procfs_loadavg_path)
        self._network = ProcfsNetwork(
            config.procfs_netstat_path,
            config.sysfs_network_prefix,
            config.netlink,
            config.metrics.poll_interval_sec,
            alpha,
        )

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
        to_return = {
            'poll_inteval_sec': self._config.metrics.poll_interval_sec
        }

        try:
            cpu = self._cpu.read()
            to_return.update({
                'cpu.load': cpu.load,
                'cpu.usable': cpu.usable,
            })
        except Exception as e:
            self.error('failed to get system metrics [cpu] {}', e)

        try:
            memory = self._memory.read()
            to_return.update({
                'mem.load': memory.load,
                'mem.usable': memory.usable,
                'mem.cached': memory.cached,
                'mem.free': memory.free,
                'mem.used': memory.used,
                'mem.total': memory.total,
                'mem.free_and_cached_ma': memory.free_and_cached_ma,
            })
        except Exception as e:
            self.error('failed to get system metrics [memory] {}', e)

        try:
            loadavg = self._loadavg.read()
            to_return.update({'loadavg': loadavg})
        except Exception as e:
            self.error('failed to get system metrics [loadavg] {}', e)

        try:
            network = self._network.read()
            network = ProcfsNetwork.as_named_dict(network)
            to_return.update({'network': network})
        except Exception as e:
            self.error('failed to get system metrics [network] {}', e)

        return to_return
