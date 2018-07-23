"""Collection of procfs metrics objects and utilities.

Links:
 - read `man 5 proc` for procfs stat files formats.
"""
import itertools
import six

from collections import namedtuple

from .ewma import EWMA
from .exceptions import MetricsException

from ..common import clamp
from ..mixins import LoggerMixin


CPU_FIELDS_COUNT = 11  # one label field "cpu" + 10 ticks fields
NET_FIELDS_COUNT = 17


class ProcfsMetric(object):
    """Linux procfs metrics base class."""

    def __init__(self, path):
        """Make ProcfsMetric with specified path."""
        self._path = path
        self.open(path)

    def open(self, path):
        """Open file in unbuffered mode."""
        self._file = open(path, 'rb', 0)

    def close(self):   # pragma nocover
        """Close internal file."""
        self._file.close()

    def reopen(self):
        """Close and open file with specified in ctor path."""
        self._file.close()
        self.open(self._path)

    def read_nfirst_lines(self, to_read=1):
        """Read 'to_read' lines from start of file."""
        try:
            self._file.seek(0)
            return [self._file.readline() for _ in xrange(to_read)]
        except Exception as e:
            self.reopen()
            raise

    def read_all_lines(self, to_skip=0):
        """Read all lines from start of file until first non empty found."""
        try:
            self._file.seek(0)
            return self._file.readlines()[to_skip:]
        except Exception as e:
            self.reopen()
            raise

    @property
    def path(self):
        """Actual path of metrics reader."""
        return self._path


class Loadavg(ProcfsMetric):
    """Loadavg metrics."""

    LOADAVG_COUNT = 3  # (1, 5, 15) minutes

    def __init__(self, path):
        """Init procfs metrics parser from string."""
        super(Loadavg, self).__init__(path)

    def read(self):
        """Read tuple of system load avarage from procfs.

        Reads tuple of system load avarage counted for 1, 5, 15 minutes
        intervals.

        :rtype: (float, float, float)
        """
        return Loadavg._parse(self.read_nfirst_lines())

    @staticmethod
    def _parse(lines):
        return [float(v) for v in lines[0].split()[:Loadavg.LOADAVG_COUNT]]


class Cpu(ProcfsMetric):
    """CPU metrics from /proc/stat."""

    #
    # Sources:
    #   - http://www.linuxhowtos.org/System/procstat.htm
    #   - https://github.com/Leo-G/DevopsWiki/wiki/
    #           How-Linux-CPU-Usage-Time-and-Percentage-is-calculated
    #   - man 5 proc
    #
    # USR: normal processes executing in user mode
    # NICE: niced processes executing in user mode
    # SYS: processes executing in kernel mode
    # IDLE: twiddling thumbs
    # IOW: waiting for I/O to complete
    # IRQ: servicing interrupts
    # SOFTIRQ: servicing softirqs
    # STEAL: involuntary wait
    # GUEST: running a normal guest
    # GUEST_NICE: running a niced guest
    #
    # Note: GUEST and GUEST_NICE are already accounted in user and nice,
    #       hence they are not included in the total calculation
    #
    #  0    1     2    3     4    5    6        7      8      9          10
    NAME, USR, NICE, SYS, IDLE, IOW, IRQ, SOFTIRQ, STEAL, GUEST, GUEST_NICE = \
        xrange(CPU_FIELDS_COUNT)

    CpuTicks = namedtuple('CpuTicks', [
        'total',
        'usage',
        'idle',
        'load',
        'usable',
    ])

    def __init__(self, path, alpha):
        """Set metrics reader from 'path'."""
        super(Cpu, self).__init__(path)

        self._prev_cpu = None
        self._load_ma, self._usable_ma = EWMA(alpha), EWMA(alpha)

    def read(self):
        """CPU load based on procfs 'stat' file.

        :return: named tuple with fields:
          total - cumulative ticks count of all cpus in `to_sleep` interval
          usage - non idle ticks count of all cpus in `to_sleep` interval
          idle - idle ticks count
          load - usage / total ratio
        :rtype: Cpu.CpuTicks
        """
        cpu = Cpu._parse(self.read_nfirst_lines())

        if self._prev_cpu is None:
            self._prev_cpu = cpu
            return cpu

        usage = cpu.usage - self._prev_cpu.usage
        idle = cpu.idle - self._prev_cpu.idle
        total = cpu.total - self._prev_cpu.total

        self._prev_cpu = cpu

        # Overflow cases (should appear on few billions of years)
        if usage < 0:
            usage = 0

        if total < 0:
            total = 0

        if idle < 0:
            idle = 0

        if not total:
            return Cpu.CpuTicks(total, usage, idle, .0, 1.0)

        self._load_ma.update(usage)
        self._usable_ma.update(idle)

        load = clamp(self._load_ma.value / float(total), .0, 1.0)
        usable = clamp(self._usable_ma.value / float(total), .0, 1.0)

        return Cpu.CpuTicks(total, usage, idle, load, usable)

    @staticmethod
    def _parse(lines):
        if not lines:  # pragma nocover
            raise MetricsException('no lines in procfs file')

        # Calculations are based on first line.
        cpu_line = lines[0]

        if not cpu_line:
            raise MetricsException('empty line in procfs file')

        splitted = cpu_line.split()

        total = sum(int(v) for v in splitted[Cpu.USR:Cpu.STEAL + 1])

        #
        # `idle` originally was:
        #
        #  idle = int(splitted[Cpu.IDLE]) + int(splitted[Cpu.IOW])
        #
        # but we have decided to account IOW ticks in usage, as it could
        # be valuable to account IO stat in scheduling process.
        #
        idle = int(splitted[Cpu.IDLE])
        usage = total - idle

        return Cpu.CpuTicks(total, usage, idle, .0, .0)


class Memory(ProcfsMetric):
    """Memory metrics from /proc/memifo."""

    TOTAL, FREE, _AVAIL, _BUFFERS, CACHED = xrange(5)
    NAME_FIELD, VALUE_FIELD, UNITS_FIELD = xrange(3)

    #
    # Hardcoded in kernel.
    #
    TOTAL_FIELD_NAME = 'MemTotal:'
    FREE_FIELD_NAME = 'MemFree:'
    CACHED_FIELD_NAME = 'Cached:'

    SUFFIX = 'kB'

    Mem = namedtuple('Mem', [
        'total',
        'free',
        'cached',
        'used',
        'load',
        'usable',
        'free_and_cached_ma',
    ])

    def __init__(self, path, alpha):
        """Init procfs memory metric with specified path."""
        super(Memory, self).__init__(path)
        self._free_and_cached_ma = EWMA(alpha)

    def read(self):
        """Read memory stat from procfs.

        :return: named tuple with bytes count - 'total', 'free', 'used',
        plus memory 'load' ratio.
        :rtype: Memory.Mem
        """
        mem = Memory._parse(self.read_nfirst_lines(5))

        self._free_and_cached_ma.update(mem.free + mem.cached)

        load_ma, usable_ma = self._calc_load_ma(mem)
        return Memory.Mem(
            mem.total,
            mem.free,
            mem.cached,
            mem.used,
            load_ma,
            usable_ma,
            self._free_and_cached_ma.int_of_value,
        )

    @staticmethod
    def _parse(lines):
        splitted = [ln.split() for ln in lines]

        Memory._assert_splitted(splitted)

        vals = [int(val[Memory.VALUE_FIELD]) for val in splitted]

        total_kb = vals[Memory.TOTAL]
        free_kb = vals[Memory.FREE]
        cached_kb = vals[Memory.CACHED]
        used_kb = vals[Memory.TOTAL] - vals[Memory.FREE]

        return Memory.Mem(
            total_kb << 10,
            free_kb << 10,
            cached_kb << 10,
            used_kb << 10,
            .0,
            .0,
            .0,
        )

    @staticmethod
    def _assert_splitted(splitted):
        assert splitted[Memory.TOTAL][Memory.NAME_FIELD] == \
            Memory.TOTAL_FIELD_NAME
        assert splitted[Memory.FREE][Memory.NAME_FIELD] == \
            Memory.FREE_FIELD_NAME
        assert splitted[Memory.CACHED][Memory.NAME_FIELD] == \
            Memory.CACHED_FIELD_NAME

        assert splitted[Memory.TOTAL][Memory.UNITS_FIELD] == \
            Memory.SUFFIX
        assert splitted[Memory.FREE][Memory.UNITS_FIELD] == \
            Memory.SUFFIX
        assert splitted[Memory.CACHED][Memory.UNITS_FIELD] == \
            Memory.SUFFIX

    def _calc_load_ma(self, mem):
        """Memory (load, usable) ratios in [0, 1] interval."""
        if not mem.total:
            return .0, .0

        free = self._free_and_cached_ma.value
        load = mem.total - free

        return \
            clamp(load / float(mem.total), .0, 1.0), \
            clamp(free / float(mem.total), .0, 1.0)


class IfSpeed(ProcfsMetric):
    """Read NIC speed from sysfs.

    TODO(IfSpeed): Deprecated: it seems that there is no any portable way
    to get real network speed bandwidth, e.g. for kvm
        sysfs:/sys/class/network/*/speed
    could be unavailable (is is useless here), or it could be set incorrectly
    for virtual interfaces in container (arguable, needs investigation).
    """

    SYSFS_FNAME = 'speed'

    def __init__(self, path):
        """Init procfs network metric with specified path."""
        super(IfSpeed, self).__init__(path)

    def read(self):
        """Read netlink speed in Mbs."""
        return IfSpeed._parse(self.read_nfirst_lines())

    @staticmethod
    def _parse(lines):
        return int(lines[0])

    @staticmethod
    def make_path(prefix, iface):
        """Construct sysfs path for iface speed file."""
        return '{}/{}/{}'.format(prefix, iface, IfSpeed.SYSFS_FNAME)


class Network(ProcfsMetric):
    """Read network stat from /proc/net/dev.

    TODO: it should be configurable interfaces name (prefix) or way to obtain
    correct config, not `node_summary` and error prone IGNORE_LIST_PFX checks.
    Seems that interface along with speed limit should be set by admin
    explicitly in config, as we don't have raliable way to obtain it from
    container.
    """

    #     0
    # Iface,
    #        1         2       3         4        5         6       7      8
    #    bytes,  packets, errors,    drops,    fifo,    frame,   comp, mcast,
    #                                                                     16
    IF_NAME, \
        RX_BTS, RX_PCKS, RX_ERR, RX_DROPS, RX_FIFO, RX_FRAME, RX_CMP, RX_MC, \
        TX_BTS, TX_PCKS, TX_ERR, TX_DROPS, TX_FIFO, TX_FRAME, TX_CMP, TX_MC  \
        = xrange(NET_FIELDS_COUNT)

    IGNORE_LIST_PFX = {'lo', 'tun', 'dummy', 'docker', 'wlan'}
    UNKNOWN_SPEED = -1

    Net = namedtuple('Net', 'speed_mbits rx tx rx_bps tx_bps')
    Rates = namedtuple('Rates', 'rx_ma tx_ma')

    def __init__(
            self, path, sysfs_path_pfx, netlink_conf,
            poll_interval_sec, alpha):
        """Init procfs network metric with specified path."""
        super(Network, self).__init__(path)

        self._speed_file_pfx = sysfs_path_pfx
        self._speed_files_cache = {}

        self._netlink_speed_mbits = netlink_conf.speed_mbits
        self._default_netlink = netlink_conf.default_name

        self._poll_inteval_sec = 1
        if poll_interval_sec:
            self._poll_interval_sec = poll_interval_sec

        self._rates = {}
        self._prev_stat = {}

        self._alpha = alpha

    def read(self):
        """Read network stat from procfs.

        :return: dictionary of inftercase with their stat
        :rtype: dict[str, Network.Net]
        """
        networks = Network._parse(self.read_all_lines(to_skip=2))

        results = {}
        for iface, net in six.iteritems(networks):
            self._fill_results(results, iface, net)

        return results

    def _fill_results(self, results, iface, net):
        rates = self.rates(iface)

        net_prev = self._prev_stat.get(iface)
        if net_prev:
            span = self._poll_interval_sec

            rates.rx_ma.update((net.rx - net_prev.rx) / float(span))
            rates.tx_ma.update((net.tx - net_prev.tx) / float(span))

        if_speed = Network.UNKNOWN_SPEED

        try:
            if_speed = self._get_link_speed(iface)
        except Exception as e:
            # probably no sysfs on node, ignore silently
            pass

        if iface == self._default_netlink:
            results['node_default'] = Network.Net(
                self._netlink_speed_mbits,
                net.rx, net.tx,
                rates.rx_ma.int_of_value,
                rates.tx_ma.int_of_value,
            )

        results[iface] = self._prev_stat[iface] = Network.Net(
            if_speed,
            net.rx, net.tx,
            rates.rx_ma.int_of_value,
            rates.tx_ma.int_of_value,
        )

    def _get_link_speed(self, iface):
        """Temporary stub for iface speed acquisition.

        TODO(profs.Network): should be redesign as it is no portable way to
        aqcuire link speed.
        """
        if iface not in self._speed_files_cache:
            path = IfSpeed.make_path(self._speed_file_pfx, iface)
            self._speed_files_cache[iface] = IfSpeed(path)

        return self._speed_files_cache[iface].read()

    def rates(self, iface):
        """Get (construct) rates structure."""
        r = self._rates.get(iface)
        if r is None:
            r = Network.Rates(EWMA(self._alpha), EWMA(self._alpha))
            self._rates[iface] = r

        return r

    @staticmethod
    def _parse(lines):
        def in_black_list(s):
            return any(s.startswith(pfx) for pfx in Network.IGNORE_LIST_PFX)

        result = {}
        lines = [ln.strip() for ln in lines]

        for ln in itertools.ifilter(lambda s: not in_black_list(s), lines):
            net_info = ln.split()

            # remove colon from iface name
            net_info[Network.IF_NAME] = net_info[Network.IF_NAME][:-1]

            result[net_info[Network.IF_NAME]] = Network.Net(
                Network.UNKNOWN_SPEED,
                int(net_info[Network.RX_BTS]),
                int(net_info[Network.TX_BTS]),
                0,
                0,
            )

        return result

    @staticmethod
    def as_named_dict(d):
        """Convert mapping of namedtuples to dictionary of dictionaries."""
        return {iface: net._asdict() for iface, net in six.iteritems(d)}
