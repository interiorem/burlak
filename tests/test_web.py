import json
from collections import namedtuple

from cocaine.burlak import burlak
from cocaine.burlak.comm_state import CommittedState
from cocaine.burlak.helpers import flatten_dict, flatten_dict_rec
from cocaine.burlak.sys_metrics import SysMetricsGatherer
from cocaine.burlak.web import API_V1, WebOptions, make_url, make_web_app_v1

import mock

import pytest

import tornado.queues

from .common import make_future


TEST_UUID = 'some_correct_uuid'

TEST_VERSION = 0.1

TEST_STATE_VERSION = 42
TEST_INCOMING_STATE_VERSION = TEST_STATE_VERSION + 1

TEST_UPTIME = 100500
TEST_PORT = 10042
TEST_TS = 13

TEST_MAXRSS_KB = 16 * 1024
TEST_MAXRSS_MB = TEST_MAXRSS_KB / 1024.0

TEST_UTIME = 100500
TEST_STIME = 42

TEST_OS_LA = [1, 2, 3]


RUsage = namedtuple('RUsage', [
    'ru_maxrss',
    'ru_utime',
    'ru_stime'
])

StateRecord = namedtuple('StateRecord', [
    'workers',
    'profile'
])

test_state = {
    'app1': CommittedState.Record('STOPPED', 100500, 1, 3, 'stopped', 100500),
    'app2': CommittedState.Record('RUNNING', 100501, 2, 2, 'ok', 100501),
    'app3': CommittedState.Record('STOPPED', 100502, 3, 1, 'stopped', 100502),
    'app4': CommittedState.Record('FAILED', 100502, 3, 1, 'error', 100502),
}

incoming_state = {
    'app1': StateRecord(4, 'one'),
    'app2': StateRecord(3, 'two'),
    'app3': StateRecord(2, 'three'),
    'app4': StateRecord(1, 'four'),
}

system_metrics = {
    'load_avg': dict(m1=1, m5=2, m15=3),
    'maxrss_mb': TEST_MAXRSS_MB,
    'utime': TEST_UTIME,
    'stime': TEST_STIME,
}


test_channels = ['x', 'y', 'z']


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

    committed_state.set_incoming_state(
        incoming_state, TEST_INCOMING_STATE_VERSION, TEST_TS)
    committed_state.version = TEST_STATE_VERSION
    committed_state.channels_cache_apps = test_channels

    workers_distribution = {'app{}'.format(i): i % 4 for i in xrange(10)}

    rusage = RUsage(TEST_MAXRSS_KB, TEST_UTIME, TEST_STIME)

    mocker.patch('os.getloadavg', return_value=TEST_OS_LA)
    mocker.patch('resource.getrusage', return_value=rusage)

    metrics_gatherer = SysMetricsGatherer()
    metrics_gatherer.as_dict = mocker.Mock(return_value=system_metrics)

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

    wops = WebOptions(
        '',
        TEST_PORT,
        uptime,
        uniresis,
        committed_state,
        metrics_gatherer,
        qs,
        units,
        workers_distribution,
        mocker.Mock(),
        TEST_VERSION
    )
    return make_web_app_v1(wops)


TEST_METRICS = dict(
    queues_fill=dict(input=1, adjust=2, stop=3),
    counters=dict(
        acquisition=dict(a_cnt=2, b_cnt=3, c_cnt=4),
        state=dict(a_cnt=3, b_cnt=4, c_cnt=5),
        slayer=dict(a_cnt=4, b_cnt=5, c_cnt=6),
        resurrecter=dict(a_cnt=5, b_cnt=6, c_cnt=7),
    ),
    system=dict(
        load_avg=dict(m1=1, m5=2, m15=3),
        maxrss_mb=TEST_MAXRSS_MB,
        utime=TEST_UTIME,
        stime=TEST_STIME),
    committed_state=dict(
        running_apps_count=sum(
            1 for record in test_state.itervalues()
            if record.state == 'STARTED'
        ),
        workers_count=sum(
            record.workers for record in test_state.itervalues()
            if record.state == 'STARTED'
        ),
    ),
)


@pytest.mark.gen_test
def test_get_metrics(http_client, base_url, mocker):

    response = yield http_client.fetch(
        base_url + make_url('', API_V1, r'metrics'))

    assert response.code == 200
    assert json.loads(response.body) == TEST_METRICS


@pytest.mark.gen_test
def test_get_metrics_flatten(http_client, base_url, mocker):

    response = yield http_client.fetch(
        base_url + make_url('', API_V1, r'metrics') + '?flatten')

    metrics = json.loads(response.body)

    assert response.code == 200

    assert metrics == dict(flatten_dict(TEST_METRICS))
    assert metrics == dict(flatten_dict_rec(TEST_METRICS))


@pytest.mark.parametrize(
    'is_legacy, state_path',
    [
        (True, r'/state'),
        (False, make_url('', API_V1, r'state')),
    ]
)
@pytest.mark.gen_test
def test_get_state(http_client, base_url, is_legacy, state_path):
    response = yield http_client.fetch(
        base_url + state_path)

    loaded_state = json.loads(response.body)

    state = loaded_state if is_legacy else loaded_state.get('state', {})

    assert response.code == 200
    assert state == \
        {app: record._asdict() for app, record in test_state.iteritems()}

    if not is_legacy:
        assert loaded_state.get('version') == TEST_STATE_VERSION


@pytest.mark.parametrize(
    'is_legacy, state_path',
    [
        (True, '/state?app={}'),
        (False, make_url('', API_V1, 'state?app={}')),
    ]
)
@pytest.mark.gen_test
def test_get_state_by_app(http_client, base_url, is_legacy, state_path):
    for app in test_state:
        response = yield http_client.fetch(
            base_url + state_path.format(app))

        loaded_state = json.loads(response.body)
        state = loaded_state if is_legacy else loaded_state.get('state', {})

        assert response.code == 200
        assert state == {app: test_state[app]._asdict()}


@pytest.mark.gen_test
def test_incoming_state(http_client, base_url):
    response = yield http_client.fetch(
        base_url + make_url('', API_V1, 'incoming_state'))

    in_state = json.loads(response.body)

    assert response.code == 200
    assert in_state.get('state', {}) == {
        k: v._asdict() for k, v in incoming_state.iteritems()
    }

    assert in_state.get('version', -1) == TEST_INCOMING_STATE_VERSION
    assert in_state.get('timestamp', -1) == TEST_TS


#
# Note that info handle doesn't have API version.
#
@pytest.mark.gen_test
def test_get_info(http_client, base_url):
    response = yield http_client.fetch(
        base_url + r'/info')

    assert response.code == 200
    assert json.loads(response.body) == \
        dict(
            uuid=TEST_UUID,
            uptime=TEST_UPTIME,
            version=TEST_VERSION,
            api=API_V1)


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


@pytest.mark.gen_test
def test_distribution_all(http_client, base_url):
    response = yield http_client.fetch(
        base_url + make_url('', API_V1, r'distribution'))

    assert response.code == 200
    assert json.loads(response.body) == dict(
        app0=0,
        app1=1,
        app2=2,
        app3=3,
        app4=0,
        app5=1,
        app6=2,
        app7=3,
        app8=0,
        app9=1,
    )


@pytest.mark.gen_test
def test_distribution_none(http_client, base_url):
    response = yield http_client.fetch(
        base_url + make_url('', API_V1, r'distribution/none'))

    assert response.code == 200
    assert json.loads(response.body) == dict(
        app0=0,
        app4=0,
        app8=0,
    )


@pytest.mark.gen_test
def test_distribution_some(http_client, base_url):
    response = yield http_client.fetch(
        base_url + make_url('', API_V1, r'distribution/some'))

    assert response.code == 200
    assert json.loads(response.body) == dict(
        app1=1,
        app2=2,
        app3=3,
        app5=1,
        app6=2,
        app7=3,
        app9=1,
    )


@pytest.mark.gen_test
def test_defaults_channels(http_client, base_url):
    response = yield http_client.fetch(
        base_url + make_url('', API_V1, r'channels'))

    assert response.code == 200
    assert json.loads(response.body) == dict(apps=test_channels)
