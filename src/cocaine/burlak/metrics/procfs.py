"""Collection of procfs metrics objects and utilities.

Links:
 - read `man 5 proc` for procfs stat files formats.
"""
import itertools
import six

from collections import namedtuple

from tornado import gen

from .exceptions import MetricsException

from ..common import clamp
from ..mixins import LoggerMixin


# Time to async sleep between two cpu load measurements
CPU_POLL_TICK = .25  # 250ms
NET_POLL_TICK = .25  # 250ms
NET_TICKS_PER_SEC = 1. / NET_POLL_TICK

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

    @gen.coroutine
    def read(self):
        """Read tuple of system load avarage from procfs.

        Reads tuple of system load avarage counted for 1, 5, 15 minutes
        intervals.

        :rtype: (float, float, float)
        """
        loadavg_lines = self.read_nfirst_lines()
        raise gen.Return(Loadavg._parse(loadavg_lines))

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

    def __init__(self, path):
        """Set metrics reader from 'path'."""
        super(Cpu, self).__init__(path)

    @gen.coroutine
    def read(self, to_sleep=CPU_POLL_TICK):
        """CPU load based on procfs 'stat' file.

        :param to_sleep: gap in seconds of sequental cpu ticks poll
        :type to_sleep: int

        :return: named tuple with fields:
          total - cumulative ticks count of all cpus in `to_sleep` interval
          usage - non idle ticks count of all cpus in `to_sleep` interval
          idle - idle ticks count
          load - usage / total ratio
        :rtype: Cpu.CpuTicks
        """
        cpu1 = Cpu._parse(self.read_nfirst_lines())
        yield gen.sleep(to_sleep)
        cpu2 = Cpu._parse(self.read_nfirst_lines())

        usage = cpu2.usage - cpu1.usage
        idle = cpu2.idle - cpu1.idle
        total = cpu2.total - cpu1.total

        # Overflow cases (should appear on few billions of years)
        if usage < 0:
            usage = 0

        if total < 0:
            total = 0

        if idle < 0:
            idle = 0

        if not total:
            raise gen.Return(Cpu.CpuTicks(total, usage, idle, .0, .0))

        load = clamp(usage / float(total), .0, 1.0)
        usable = clamp(idle / float(total), .0, 1.0)

        raise gen.Return(Cpu.CpuTicks(total, usage, idle, load, usable))

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

    SUFFIXES = 'kB'

    Mem = namedtuple('Mem', [
        'total',
        'free',
        'cached',
        'used',
        'load',
    ])

    def __init__(self, path):
        """Init procfs memory metric with specified path."""
        super(Memory, self).__init__(path)

    @gen.coroutine
    def read(self):
        """Read memory stat from procfs.

        :return: named tuple with bytes count - 'total', 'free', 'used',
        plus memory 'load' ratio.
        :rtype: Memory.Mem
        """
        lines = self.read_nfirst_lines(5)
        raise gen.Return(Memory._parse(lines))

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
            Memory.calc_load(total_kb, used_kb, cached_kb),
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
            Memory.SUFFIXES
        assert splitted[Memory.FREE][Memory.UNITS_FIELD] == \
            Memory.SUFFIXES
        assert splitted[Memory.CACHED][Memory.UNITS_FIELD] == \
            Memory.SUFFIXES

    @staticmethod
    def calc_load(total, used, cached):
        """Memory load ratio in [0, 1] interval."""
        if not total:
            return .0

        if used > cached:
            # should be some kind of fault in other case.
            used = used - cached

        return clamp(used / float(total), .0, 1.0)


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

    @gen.coroutine
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

    IGNORE_LIST_PFX = {'lo', 'tun', 'dummy', 'wlan', 'docker', 'br'}
    UNKNOWN_SPEED = -1

    Net = namedtuple('Net', 'speed_mbits rx tx rx_delta tx_delta')

    def __init__(
            self,
            path, sysfs_path_pfx, default_netlink, netlink_speed_mbits):
        """Init procfs network metric with specified path."""
        super(Network, self).__init__(path)

        self._netlink_speed_mbits = netlink_speed_mbits
        self._speed_file_pfx = sysfs_path_pfx
        self._speed_files_cache = {}
        self._default_netlink = default_netlink

    @gen.coroutine
    def read(self):
        """Read network stat from procfs.

        :return: dictionary of inftercase with their stat
        :rtype: dict[str, Network.Net]
        """
        network1 = Network._parse(self.read_all_lines(to_skip=2))
        yield gen.sleep(NET_POLL_TICK)
        network2 = Network._parse(self.read_all_lines(to_skip=2))

        to_process = [
            (iface, network1[iface], network2[iface])
            for iface in network1 if iface in network2
        ]

        results = {}
        for (iface, net1, net2) in to_process:

            drx = int(NET_TICKS_PER_SEC * (net2.rx - net1.rx))
            dtx = int(NET_TICKS_PER_SEC * (net2.tx - net1.tx))

            if_speed = Network.UNKNOWN_SPEED

            try:
                if_speed = yield self._get_link_speed(iface)
            except Exception as e:
                # probably no sysfs on node, ignore silently
                pass

            if iface == self._default_netlink:
                results['node_default'] = \
                    Network.Net(
                        self._netlink_speed_mbits, net2.rx, net2.tx, drx, dtx)

            results[iface] = Network.Net(if_speed, net2.rx, net2.tx, drx, dtx)

        #
        # Count summary for all active interfaces.
        # Note that some ifaces stats could be included in some other,
        # so it is possibility of overcount, correct accounting is subject
        # of further investigation.
        #
        summary_rx, summary_tx, delta_rx, delta_tx, max_speed = 0, 0, 0, 0, 0
        for net in six.itervalues(results):
            summary_rx += net.rx
            summary_tx += net.tx

            delta_rx += net.rx_delta
            delta_tx += net.tx_delta

            if net.speed_mbits > max_speed:
                max_speed = net.speed_mbits

        if max_speed <= 0:  # e.g. KVM
            max_speed = self._netlink_speed_mbits

        results['node_summary'] = Network.Net(
            max_speed,
            summary_rx,
            summary_tx,
            delta_rx,
            delta_tx,
        )

        raise gen.Return(results)

    @gen.coroutine
    def _get_link_speed(self, iface):
        """Temporary stub for iface speed acquisition.

        TODO(profs.Network): should be redesign as it is no portable way to
        aqcuire link speed.
        """
        if iface not in self._speed_files_cache:
            path = IfSpeed.make_path(self._speed_file_pfx, iface)
            self._speed_files_cache[iface] = IfSpeed(path)

        speed = yield self._speed_files_cache[iface].read()
        raise gen.Return(speed)

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
