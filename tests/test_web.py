import json

from collections import namedtuple

from cocaine.burlak import burlak
from cocaine.burlak.comm_state import CommittedState
from cocaine.burlak.web import API_V1, make_url, make_web_app_v1

import mock
import pytest

import tornado.queues

from .common import make_future


TEST_UUID = 'some_correct_uuid'
TEST_VERSION = 0.1
TEST_STATE_VERSION = 42
TEST_UPTIME = 100500
TEST_PORT = 10042

TEST_MAXRSS_KB = 16 * 1024
TEST_MAXRSS_MB = TEST_MAXRSS_KB / 1024.0

TEST_UTIME = 100500
TEST_STIME = 42

RUsage = namedtuple('RUsage', [
    'ru_maxrss',
    'ru_utime',
    'ru_stime'
])


test_state = {
    'app1': CommittedState.Record('STOPPED', 100500, 1, 3, 100500),
    'app2': CommittedState.Record('RUNNING', 100501, 2, 2, 100501),
    'app3': CommittedState.Record('STOPPED', 100502, 3, 1, 100502),
    'app4': CommittedState.Record('FAILED', 100502, 3, 1, 100502),
}


class MetricsMock(burlak.MetricsMixin):
    def __init__(self, init, **kwargs):
        super(MetricsMock, self).__init__(**kwargs)
        self.init = init

    def get_count_metrics(self):
        self.metrics_cnt = dict(
            a_cnt=self.init + 1,
            b_cnt=self.init + 2,
            c_cnt=self.init + 3
        )

        return super(MetricsMock, self).get_count_metrics()


@pytest.fixture
def app(mocker):
    input_queue = tornado.queues.Queue()
    adjust_queue = tornado.queues.Queue()
    stop_queue = tornado.queues.Queue()

    input_queue.qsize = mock.MagicMock(return_value=1)
    adjust_queue.qsize = mock.MagicMock(return_value=2)
    stop_queue.qsize = mock.MagicMock(return_value=3)

    committed_state = CommittedState()
    committed_state.as_dict = mock.MagicMock(
        return_value=test_state
    )
    committed_state.as_named_dict = mock.MagicMock(
        return_value={
            app: record._asdict()
            for app, record in test_state.iteritems()
        }
    )

    committed_state.version = TEST_STATE_VERSION

    qs = dict(input=input_queue, adjust=adjust_queue, stop=stop_queue)
    units = dict(
            acquisition=MetricsMock(1),
            state=MetricsMock(2),
            slayer=MetricsMock(3),
            resurrecter=MetricsMock(4))

    uniresis = mocker.Mock()
    uniresis.uuid = mocker.Mock(return_value=make_future(TEST_UUID))

    uptime = mocker.Mock()
    uptime.uptime = mocker.Mock(return_value=TEST_UPTIME)

    return make_web_app_v1(
        '', TEST_PORT, uptime, uniresis, committed_state, qs, units,
        TEST_VERSION)


@pytest.mark.gen_test
def test_get_metrics(http_client, base_url, mocker):

    rusage = RUsage(TEST_MAXRSS_KB, TEST_UTIME, TEST_STIME)

    mocker.patch('os.getloadavg', return_value=[1, 2, 3])
    mocker.patch('resource.getrusage', return_value=rusage)

    response = yield http_client.fetch(
        base_url + make_url('', API_V1, r'metrics'))

    assert response.code == 200
    assert json.loads(response.body) == dict(
        queues_fill=dict(input=1, adjust=2, stop=3),
        counters=dict(
            acquisition=dict(a_cnt=2, b_cnt=3, c_cnt=4),
            state=dict(a_cnt=3, b_cnt=4, c_cnt=5),
            slayer=dict(a_cnt=4, b_cnt=5, c_cnt=6),
            resurrecter=dict(a_cnt=5, b_cnt=6, c_cnt=7),
        ),
        system=dict(
            load_avg=[1, 2, 3],
            maxrss_mb=TEST_MAXRSS_MB,
            utime=TEST_UTIME,
            stime=TEST_STIME)
    )


@pytest.mark.parametrize(
    'state_path',
    [
        r'/state',
        make_url('', API_V1, r'state'),
    ]
)
@pytest.mark.gen_test
def test_get_state(http_client, base_url, state_path):
    response = yield http_client.fetch(
        base_url + state_path)

    assert response.code == 200
    assert json.loads(response.body) == \
        {app: record._asdict() for app, record in test_state.iteritems()}


@pytest.mark.parametrize(
    'state_path',
    [
        '/state?app={}',
        make_url('', API_V1, 'state?app={}'),
    ]
)
@pytest.mark.gen_test
def test_get_state_by_app(http_client, base_url, state_path):
    for app in test_state:
        response = yield http_client.fetch(
            base_url + state_path.format(app))

        assert response.code == 200
        print 'app {} body {}'.format(app, response.body)
        assert json.loads(response.body) == {app: test_state[app]._asdict()}


#
# Note that info handle doesn't have API version.
#
@pytest.mark.gen_test
def test_get_info(http_client, base_url):
    response = yield http_client.fetch(
        base_url + r'/info')

    assert response.code == 200
    assert json.loads(response.body) == \
        {
            'uuid': TEST_UUID,
            'uptime': TEST_UPTIME,
            'version': TEST_VERSION,
            'api': API_V1,
        }


@pytest.mark.parametrize(
    'failed_path',
    [
        r'/failed',
        make_url('', API_V1, r'failed'),
    ]
)
@pytest.mark.gen_test
def test_get_failed(http_client, base_url, failed_path):
    response = yield http_client.fetch(
        base_url + failed_path)

    failed = [
        app for app, record in test_state.iteritems()
        if record.state_version == TEST_STATE_VERSION and
        record.state == 'FAILED'
    ]

    assert response.code == 200
    assert json.loads(response.body).get('failed', []) == failed
