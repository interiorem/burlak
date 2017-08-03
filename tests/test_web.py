import json

from cocaine.burlak import CommittedState
from cocaine.burlak.web import MetricsHandler, StateHandler

import mock
import pytest

import tornado.queues
import tornado.web


test_state = {
    'app1': ['STOPPED', 100500, 0],
    'app2': ['RUNNING', 100501, 1],
    'app3': ['STOPPED', 100502, 3],
}


class MetricsMock(object):
    def __init__(self, init):
        self.init = init

    def get_metrics(self):
        return dict(
            a_cnt=self.init + 1,
            b_cnt=self.init + 2,
            c_cnt=self.init + 3)


@pytest.fixture
def app():
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

    qs = dict(input=input_queue, adjust=adjust_queue, stop=stop_queue)
    units = dict(
            acquisition=MetricsMock(1),
            state=MetricsMock(2),
            slayer=MetricsMock(3),
            resurrecter=MetricsMock(4))

    return tornado.web.Application([
        (r'/state', StateHandler, dict(committed_state=committed_state)),
        (r'/metrics', MetricsHandler, dict(queues=qs, units=units))
    ])


@pytest.mark.gen_test
def test_get_metrics(http_client, base_url):
    response = yield http_client.fetch(base_url + '/metrics')
    print(response.body)
    assert response.code == 200
    assert json.loads(response.body) == dict(
        queues_fill=dict(input=1, adjust=2, stop=3),
        metrics=dict(
            acquisition=dict(a_cnt=2, b_cnt=3, c_cnt=4),
            state=dict(a_cnt=3, b_cnt=4, c_cnt=5),
            slayer=dict(a_cnt=4, b_cnt=5, c_cnt=6),
            resurrecter=dict(a_cnt=5, b_cnt=6, c_cnt=7),
        )
    )


@pytest.mark.gen_test
def test_get_state(http_client, base_url):
    response = yield http_client.fetch(base_url + '/state')
    print(response.body)
    assert response.code == 200
    assert json.loads(response.body) == test_state
