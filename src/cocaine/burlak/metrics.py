#
# TODO:
#   - Tests! Not tested at all!
#   - Merge with sys_metrics.py, mostly duplicated code with same functionality.
#
import time

from collections import namedtuple
from tornado import gen

from .loop_sentry import LoopSentry
from .mixins import LoggerMixin


# Time to async sleep between two cpu load measurements
CPU_POLL_TICK = .25
CPU_FIELDS_COUNT = 1 + 10


def clamp(v, low=0., hi=1.):
    return min(max(v, low), hi)


def read_nfirst_lines(f, to_read=1):
    f.seek(0)
    return [f.readline() for _ in xrange(to_read)]


def parse_loadavg(lines):
    return [float(v) for v in lines[0].split()[:3]]


@gen.coroutine
def calc_cpu_load(f, to_sleep=CPU_POLL_TICK):
    cpu1 = RCpuParser.parse(read_nfirst_lines(f, 1))
    yield gen.sleep(to_sleep)
    cpu2 = RCpuParser.parse(read_nfirst_lines(f, 1))

    total = float(cpu2.total - cpu1.total)
    usage = float(cpu2.useful - cpu1.useful)

    if total:
        ratio = usage / total
        ratio = clamp(ratio, .0, 1.0)

        raise gen.Return(ratio)
    else:
        raise gen.Return(.0)


class RCpuParser(object):

    USEFUL_COUNT = 8

    #  0     1     2    3     4   5    6        7      8      9          10
    NAME, USER, NICE, SYS, IDLE, IO, IRQ, SOFTIRQ, STEAL, GUEST, GUEST_NICE = \
        xrange(CPU_FIELDS_COUNT)

    CPU_PFX = 'cpu'

    CpuTicks = namedtuple('CpuTicks', [
        'total',
        'useful'
    ])

    @staticmethod
    def parse(lines):

        if not lines:
            return RCpuParser.CpuTicks(0, 0)

        cpu_line = next(iter(lines or []), None)

        if cpu_line is None:
            return RCpuParser.CpuTicks(0, 0)

        splitted = cpu_line.split()

        if len(splitted) != CPU_FIELDS_COUNT:
            return RCpuParser.CpuTicks(0, 0)

        total = sum(
            int(v)
            for v in splitted[RCpuParser.USER:RCpuParser.USEFUL_COUNT + 1]
        )

        idle = int(splitted[RCpuParser.IDLE]) + int(splitted[RCpuParser.IO])
        useful = total - idle

        return RCpuParser.CpuTicks(total, useful)


class RMemParser(object):
    TOTAL, FREE = xrange(2)
    NAME_FIELD, VALUE_FIELD, UNITS_FIELD = xrange(3)

    # Hardcoded in kernel.
    TOTAL_FIELD_NAME = 'MemTotal:'
    FREE_FIELD_NAME = 'MemFree:'
    SUFFIXES = 'kB'

    Mem = namedtuple('Mem', 'total_kb free_kb used_kb')

    @staticmethod
    def parse(lines):
        splitted = [ln.split() for ln in lines]

        RMemParser._assert_splitted(splitted)

        vals = [int(val[RMemParser.VALUE_FIELD]) for val in splitted]

        return RMemParser.Mem(
            vals[RMemParser.TOTAL],
            vals[RMemParser.FREE],
            vals[RMemParser.TOTAL] - vals[RMemParser.FREE],
        )

    @staticmethod
    def _assert_splitted(splitted):
        assert splitted[RMemParser.TOTAL][RMemParser.NAME_FIELD] == \
            RMemParser.TOTAL_FIELD_NAME
        assert splitted[RMemParser.FREE][RMemParser.NAME_FIELD] == \
            RMemParser.FREE_FIELD_NAME

        assert splitted[RMemParser.TOTAL][RMemParser.UNITS_FIELD] == \
            RMemParser.SUFFIXES
        assert splitted[RMemParser.FREE][RMemParser.UNITS_FIELD] == \
            RMemParser.SUFFIXES

    # TODO: not part of parser api
    @staticmethod
    def load(mem):
        if not mem.total_kb:
            return .0

        ratio = mem.used_kb / float(mem.total_kb)
        return clamp(ratio, .0, 1.0)


class ProcFiles(object):

    def __init__(self, context):
        self._config = context.config
        self.open_all(context.config)

    def open_all(self, config):
        self._stat = open(config.procfs_stat_name, 'rb', 0)
        self._mem = open(config.procfs_mem_name, 'rb', 0)
        self._loadavg = open(config.procfs_loadavg_name, 'rb', 0)

    def close_all(self):
        self._loadavg.close()
        self._mem.close()
        self._stat.close()

    def reopen_all(self):
        try:
            self.close_all(self._config)
            self.open_all()
        except:
            pass

    @property
    def loadavg(self):
        return self._loadavg

    @property
    def meminfo(self):
        return self._mem

    @property
    def stat(self):
        return self._stat


class SystemMetrics(LoggerMixin, LoopSentry):
    """Gets system wide metrics

    TODO: overlaps with sys_metrics.py code, should be merged someday.
    """
    def __init__(self, context, **kwargs):
        super(SystemMetrics, self).__init__(context, **kwargs)

        self._config = context.config

        self._proc = ProcFiles(context)

        # System metrics
        self._load_avg = .0, .0, .0
        self._cpu_load = .0

        self._mem = RMemParser.Mem(0, 0, 0)

    @gen.coroutine
    def poll(self):
        start = time.time()

        should_reopen = False

        try:
            loadavg_lines = read_nfirst_lines(self._proc.loadavg)
            self._load_avg = parse_loadavg(loadavg_lines)
        except Exception as e:
            should_reopen = True
            self.error('failed to get loadavg metrics {}', e)

        try:
            mem_lines = read_nfirst_lines(self._proc.meminfo, 2)
            self._mem = RMemParser.parse(mem_lines)
        except Exception as e:
            should_reopen = True
            self.error('failed to get system memory metrics {}', e)

        try:
            self._cpu_load = yield calc_cpu_load(self._proc.stat)
        except Exception as e:
            should_reopen = True
            self.error('failed to get system cpu load metrics {}', e)

        self.debug(
            'system load metrics fetched, time {:.3f}s', time.time() - start)

        if should_reopen:
            self._proc.reopen_all()

    @property
    def loadavg(self):
        return self._load_avg

    @property
    def cpu_load(self):
        return self._cpu_load

    @property
    def mem_load(self):
        return RMemParser.load(self._mem)

    @property
    def mem_used(self):
        return self._mem.used_kb << 10

    @property
    def mem_free(self):
        return self._mem.free_kb << 10

    @property
    def mem_total(self):
        return self._mem.total_kb << 10


class BasicMetrics(LoggerMixin):
    """Basic workers metrics provider
    """
    def __init__(self, context, _service_name, sys_metrics, **kwargs):
        super(BasicMetrics, self).__init__(context, **kwargs)

        self._service_name = _service_name
        self._sys_metrics = sys_metrics

    @gen.coroutine
    def fetch(self, _query):
        yield self._sys_metrics.poll()

        raise gen.Return({
            'loadavg': self._sys_metrics.loadavg,
            'cpu.load': self._sys_metrics.cpu_load,
            'mem.load': self._sys_metrics.mem_load,
            'mem.free': self._sys_metrics.mem_free,
            'mem.used': self._sys_metrics.mem_used,
            'mem.total': self._sys_metrics.mem_total,
        })
