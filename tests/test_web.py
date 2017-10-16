import json

from cocaine.burlak import burlak
from cocaine.burlak.comm_state import CommittedState
from cocaine.burlak.web import make_web_app

import mock
import pytest

import tornado.queues

from .common import make_future


TEST_UUID = 'some_correct_uuid'
TEST_VERSION = 0.1
TEST_UPTIME = 100500
TEST_PORT = 10042

test_state = {
    'app1': CommittedState.Record('STOPPED', 100500, 1, 3, 100500),
    'app2': CommittedState.Record('RUNNING', 100501, 2, 2, 100501),
    'app3': CommittedState.Record('STOPPED', 100502, 3, 1, 100502),
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

    return make_web_app(
        '', TEST_PORT, uptime, uniresis, committed_state, qs, units,
        TEST_VERSION)


@pytest.mark.gen_test
def test_get_metrics(http_client, base_url, mocker):

    mocker.patch('os.getloadavg', return_value=[1, 2, 3])

    response = yield http_client.fetch(base_url + '/metrics')
    assert response.code == 200
    assert json.loads(response.body) == dict(
        queues_fill=dict(input=1, adjust=2, stop=3),
        counters=dict(
            acquisition=dict(a_cnt=2, b_cnt=3, c_cnt=4),
            state=dict(a_cnt=3, b_cnt=4, c_cnt=5),
            slayer=dict(a_cnt=4, b_cnt=5, c_cnt=6),
            resurrecter=dict(a_cnt=5, b_cnt=6, c_cnt=7),
        ),
        system=dict(load_avg=[1, 2, 3])
    )


@pytest.mark.gen_test
def test_get_state(http_client, base_url):
    response = yield http_client.fetch(base_url + '/state')

    assert response.code == 200
    assert json.loads(response.body) == \
        {app: record._asdict() for app, record in test_state.iteritems()}


@pytest.mark.gen_test
def test_get_state_by_app(http_client, base_url):
    for app in test_state:
        response = yield http_client.fetch(
            base_url + '/state?app={}'.format(app))

        assert response.code == 200
        print 'app {} body {}'.format(app, response.body)
        assert json.loads(response.body) == {app: test_state[app]._asdict()}


@pytest.mark.gen_test
def test_get_uuid(http_client, base_url):
    response = yield http_client.fetch(base_url + '/info')

    assert response.code == 200
    assert json.loads(response.body) == \
        {
            'uuid': TEST_UUID,
            'uptime': TEST_UPTIME,
            'version': TEST_VERSION,
        }
