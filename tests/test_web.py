from cocaine.burlak import CommittedState
from cocaine.burlak.web import MetricsHandler, StateHandler

import pytest

import tornado.queues
import tornado.web


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

    committed_state = CommittedState()

    qs = dict(input=input_queue, adjust=adjust_queue, stop=stop_queue)
    units = dict(
            acquisition=MetricsMock(1),
            state=MetricsMock(2),
            slayer=MetricsMock(3),
            baptizer=MetricsMock(4))

    return tornado.web.Application([
        (r'/state', StateHandler, dict(committed_state=committed_state)),
        (r'/metrics', MetricsHandler, dict(queues=qs, units=units))
    ])


@pytest.mark.gen_test
def test_get_metrics(http_client, base_url):
    response = yield http_client.fetch(base_url + '/metrics')
    assert response.code == 200


@pytest.mark.gen_test
def test_get_state(http_client, base_url):
    response = yield http_client.fetch(base_url + '/state')
    assert response.code == 200
