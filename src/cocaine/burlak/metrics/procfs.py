"""Collection of procfs metrics objects and utilities.

Links:
 - read `man 5 proc` for procfs stat files formats.
"""
from collections import namedtuple

from tornado import gen

from .exceptions import MetricsException

from ..common import clamp
from ..mixins import LoggerMixin


# Time to async sleep between two cpu load measurements
CPU_POLL_TICK = .25  # 250ms
CPU_FIELDS_COUNT = 11  # one label field "cpu" + 10 ticks fields


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
