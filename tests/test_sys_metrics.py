from collections import namedtuple

from cocaine.burlak.loop_sentry import LoopSentry
from cocaine.burlak.sys_metrics import SysMetricsGatherer

import pytest

from .common import ASYNC_TESTS_TIMEOUT


TEST_MAXRSS_KB = 16 * 1024
TEST_MAXRSS_MB = TEST_MAXRSS_KB / 1024.0

TEST_UTIME = 100500
TEST_STIME = 42

TEST_LA = [1, 2, 3]


ResourceUsage = namedtuple('ResourceUsage', [
    'ru_maxrss',
    'ru_utime',
    'ru_stime',
])


@pytest.fixture
def metrics_gatherer():
    return SysMetricsGatherer()


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_gather(metrics_gatherer, mocker):
    mocker.patch.object(
        LoopSentry,
        'should_run',
        side_effect=[True, False])

    mocker.patch('os.getloadavg', return_value=TEST_LA)
    mocker.patch(
        'resource.getrusage',
        return_value=ResourceUsage(TEST_MAXRSS_KB, TEST_UTIME, TEST_STIME))

    yield metrics_gatherer.gather()

    assert metrics_gatherer.as_dict() == dict(
        load_avg=TEST_LA,
        maxrss_mb=TEST_MAXRSS_MB,
        utime=TEST_UTIME,
        stime=TEST_STIME
    )


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_as_dict(metrics_gatherer):
    assert metrics_gatherer.as_dict() == dict(
        load_avg=[0.0 for _ in xrange(0, 3)],
        maxrss_mb=0.0,
        utime=0.0,
        stime=0.0
    )
