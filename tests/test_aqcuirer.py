from cocaine.burlak import burlak
from cocaine.burlak.uniresis import catchup_an_uniresis

import mock
import pytest

from tornado import queues

from .common import ASYNC_TESTS_TIMEOUT
from .common import make_logger_mock, make_mock_channel_with


TEST_UUID_PFX = '/test_uuid_prefix'
TEST_UUID = 'test_uuid1'

apps_lists = [
    ('app1', 'app2', 'app3'),
    ('app3', 'app4', 'app5'),
    ('app5', 'app6'),
]

apps_lists_ecxpt = [
    Exception('some', 'error0'),
    ('app2', 'app3'),
    Exception('some', 'error1'),
    ('app3', 'app4'),
    Exception('some', 'error2'),
]


states_list = [
    (dict(
        app1=(1, 'SomeProfile1'),
        app2=(2, 'SomeProfile2'),
        app3=(3, 'SomeProfile3'),
    ), 0),
    (dict(
        app3=(3, 'SomeProfile3'),
        app4=(4, 'SomeProfile4'),
        app5=(5, 'SomeProfile5'),
    ), 1),
]


@pytest.fixture
def acq(mocker):
    logger = make_logger_mock(mocker)
    input_queue = queues.Queue()

    return burlak.StateAcquirer(
        burlak.LoggerSetup(logger, False),
        input_queue)


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_state_subscribe_input(acq, mocker):

    stop_side_effect = [True for _ in states_list]
    stop_side_effect.append(True)
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    unicorn = mocker.Mock()
    unicorn.subscribe = mock.Mock(
        side_effect=[make_mock_channel_with(*states_list)]
    )

    node = mocker.Mock()
    node.list = mock.Mock(
        side_effect=[
            make_mock_channel_with(*[k for k in state.iterkeys()])
            for (state, _) in states_list
        ]
    )

    uniresis = catchup_an_uniresis(use_stub_uuid=TEST_UUID)

    for state, ver in states_list:
        yield acq.subscribe_to_state_updates(
            unicorn, node, uniresis, TEST_UUID_PFX)

        inp = yield acq.input_queue.get()
        acq.input_queue.task_done()

        assert isinstance(inp, burlak.StateUpdateMessage)

        assert inp.get_state() == state
        assert inp.get_version() == ver
