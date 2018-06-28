import sys

from cocaine.burlak import config
from cocaine.burlak.context import Context, LoggerSetup
from cocaine.burlak.metrics.exceptions import MetricsException
from cocaine.burlak.metrics.fetcher import MetricsFetcher
from cocaine.burlak.metrics.procfs import Cpu, Loadavg, Memory, ProcfsMetric
from cocaine.burlak.metrics.system import SystemMetrics

import pytest

from .common import ASYNC_TESTS_TIMEOUT
from .common import make_future, make_logger_mock


if (sys.version_info < (3, 0)):
    OPEN_TO_PATCH = '__builtin__.open'
else:
    OPEN_TO_PATCH = 'builtins.open'


metrics_poll_result = {
    'cpu': 0.5,
    'mem': 100500,
    'mem.load': 1.2,
    'mem.total': 42,
}

CPU_STAT_EMPTY_CONTENT = ['cpu  0 0 0 0 0 0 0 0 0 0']

CPU_STAT_CONTENT_1 = ['cpu  1 2 3 0 2 0 1 0 0 0']
CPU_STAT_CONTENT_2 = ['cpu  5 4 3 2 2 0 1 0 0 0']

CPU_LOAD = .75

MEMINFO_CONTENT_1 = [
    'MemTotal:         100500 kB',
    'MemFree:             128 kB',
    'MemAvailable:         42 kB',
    'Buffers:               1 kB',
    'Cached:             1024 kB',
]

MEMINFO_ZEROES = [
    'MemTotal:              0 kB',
    'MemFree:               0 kB',
    'MemAvailable:          0 kB',
    'Buffers:               0 kB',
    'Cached:                0 kB',
]

MEMINFO_INVALID_CACHED = [
    'MemTotal:            200 kB',
    'MemFree:             100 kB',
    'MemAvailable:          3 kB',
    'Buffers:               1 kB',
    'Cached:              110 kB',
]


MEM_TOTAL_BYTES = 100500 << 10
MEM_FREE_BYTES = 128 << 10
MEM_CACHED_BYTES = 1024 << 10
MEM_USED_BYTES = (MEM_TOTAL_BYTES - MEM_FREE_BYTES)


MEMINFO_CONTENT_2 = [
    'MemTotal:         100501 kB',
    'MemFree:             256 kB',
    'MemAvailable:         32 kB',
]


LOADAVG_CONTENT = [
    '1.02 0.99 1.00 2/1188 9164'
]


class _MockFile(object):
    def __init__(self, data1, data2=None, should_throw=False):
        self._data = data1
        self._data_after = data2

        self._limit = len(data1)
        self._index = 0

        self._should_throw = should_throw

    def seek(self, offset):
        # Note: for testing broken simantics - offset is a index,
        # not bytes offset
        self._index = 0

    def readline(self):
        ln = self._data[self._index]
        self._index += 1

        if self._should_throw:
            raise Exception('fuh!')

        if self._index >= self._limit:
            if self._data_after:
                self._data, self._data_after = self._data_after, self._data
            self._index = 0

        return ln

    def close(self):
        pass


@pytest.fixture
def context(mocker):
    logger = make_logger_mock(mocker)
    sentry_wrapper = mocker.Mock()

    return Context(
        LoggerSetup(logger, False),
        config.Config(mocker.Mock()),
        '0',
        sentry_wrapper,
        mocker.Mock(),
    )


@pytest.fixture
def system_metrics(mocker, context):
    return SystemMetrics(context)


@pytest.fixture
def fetcher(mocker, context, system_metrics):
    return MetricsFetcher(context, system_metrics)


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_fetcher(mocker, fetcher):
    fetcher._system_metrics.poll = \
        mocker.Mock(return_value=make_future(metrics_poll_result))

    metrics = yield fetcher.fetch({})

    assert 'system' in metrics
    assert metrics['system'] == metrics_poll_result


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_system_metrics(mocker, context):

    mocker.patch(
        OPEN_TO_PATCH,
        side_effect=[
            _MockFile(CPU_STAT_CONTENT_1, CPU_STAT_CONTENT_2),
            _MockFile(MEMINFO_CONTENT_1),
            _MockFile(LOADAVG_CONTENT),
        ]
    )

    system_metrics = SystemMetrics(context)

    metrics = yield system_metrics.poll()
    metrics_set = set([
        'loadavg',
        'cpu.load',
        'cpu.usable',
        'mem.free',
        'mem.cached',
        'mem.load',
        'mem.used',
        'mem.total',
    ])

    assert metrics.viewkeys() == metrics_set

    eps = .02

    assert metrics['loadavg'][0] == pytest.approx(1.02, eps)
    assert metrics['loadavg'][1] == pytest.approx(0.99, eps)
    assert metrics['loadavg'][2] == pytest.approx(1.00, eps)

    assert metrics['cpu.load'] == pytest.approx(CPU_LOAD, eps)
    assert metrics['cpu.usable'] == pytest.approx(1.0 - CPU_LOAD, eps)

    assert metrics['mem.total'] == MEM_TOTAL_BYTES
    assert metrics['mem.free'] == MEM_FREE_BYTES
    assert metrics['mem.cached'] == MEM_CACHED_BYTES


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_system_metrics_exception(mocker, context):
    cpu_exception = make_future(Exception('Boom CPU'))
    mem_exception = make_future(Exception('Boom Mem'))
    la_exception = make_future(Exception('Boom LA'))

    mocker.patch.object(Cpu, 'read', return_value=cpu_exception)
    mocker.patch.object(Memory, 'read', return_value=mem_exception)
    mocker.patch.object(Loadavg, 'read', return_value=la_exception)

    system_metrics = SystemMetrics(context)

    metrics = yield system_metrics.poll()
    assert metrics == {}


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_procfs_multiple_times(mocker, context):

    mocker.patch(
        OPEN_TO_PATCH,
        return_value=_MockFile(CPU_STAT_CONTENT_1, CPU_STAT_CONTENT_2)
    )

    p = ProcfsMetric('/profs/stat')

    assert p.read_nfirst_lines(1) == CPU_STAT_CONTENT_1
    assert p.read_nfirst_lines(1) == CPU_STAT_CONTENT_2
    assert p.read_nfirst_lines(1) == CPU_STAT_CONTENT_1


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_procfs_multiple_lines(mocker, context):
    mocker.patch(
        OPEN_TO_PATCH,
        return_value=_MockFile(MEMINFO_CONTENT_1, MEMINFO_CONTENT_2)
    )

    p = ProcfsMetric('/profs/stat')

    assert p.read_nfirst_lines(1) == MEMINFO_CONTENT_1[:1]
    assert p.read_nfirst_lines(2) == MEMINFO_CONTENT_1[:2]
    assert p.read_nfirst_lines(3) == MEMINFO_CONTENT_1[:3]
    assert p.read_nfirst_lines(5) == MEMINFO_CONTENT_1[:5]
    assert p.read_nfirst_lines(2) == MEMINFO_CONTENT_2[:2]


@pytest.mark.xfail(raises=MetricsException)
@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_cpu_read_exceptions(mocker):
    c = Cpu('/dev/null')
    yield c.read()


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_cpu_empty_record(mocker):
    mocker.patch(
        OPEN_TO_PATCH,
        return_value=_MockFile(CPU_STAT_EMPTY_CONTENT, CPU_STAT_EMPTY_CONTENT)
    )

    c = Cpu('/proc/stat')
    c = yield c.read()

    assert isinstance(c.total, int)
    assert isinstance(c.usage, int)
    assert isinstance(c.load, float)

    assert c.total == 0
    assert c.usage == 0
    assert c.load == .0


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_cpu_overflow(mocker):
    mocker.patch(
        OPEN_TO_PATCH,
        return_value=_MockFile(CPU_STAT_CONTENT_2, CPU_STAT_CONTENT_1)
    )

    c = Cpu('/proc/stat')
    c = yield c.read()

    assert isinstance(c.total, int)
    assert isinstance(c.usage, int)
    assert isinstance(c.load, float)

    assert c.total == 0
    assert c.usage == 0
    assert c.load == .0


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_mem_zeroes_record(mocker):
    mocker.patch(
        OPEN_TO_PATCH,
        return_value=_MockFile(MEMINFO_ZEROES)
    )

    m = Memory('/proc/stat')
    m = yield m.read()

    assert isinstance(m.total, int)
    assert isinstance(m.free, int)
    assert isinstance(m.cached, int)
    assert isinstance(m.used, int)
    assert isinstance(m.load, float)

    assert m.total == 0
    assert m.free == 0
    assert m.cached == 0
    assert m.used == 0
    assert m.load == 0


def test_procfs_metrics(mocker):
    mocker.patch(
        OPEN_TO_PATCH,
        return_value=_MockFile(MEMINFO_CONTENT_1)
    )

    test_path = '/procfs/some/file'

    p = ProcfsMetric(test_path)
    ln = p.read_nfirst_lines(2)
    assert ln == MEMINFO_CONTENT_1[:2]

    ln = p.read_nfirst_lines(2)
    assert ln == MEMINFO_CONTENT_1[:2]

    p.reopen()
    ln = p.read_nfirst_lines(5)
    assert ln == MEMINFO_CONTENT_1

    assert p.path == test_path


@pytest.mark.xfail(raises=Exception)
def test_procfs_metrics_exception(mocker):
    mocker.patch(
        OPEN_TO_PATCH,
        return_value=_MockFile(MEMINFO_CONTENT_1, should_throw=True)
    )

    test_path = '/procfs/some/file'

    p = ProcfsMetric(test_path)
    p.read_nfirst_lines(2)
