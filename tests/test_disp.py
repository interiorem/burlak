#
# TODO: more test for StateUpdateMessage
#
from cocaine.burlak import burlak

import pytest

from tornado import queues

from .common import ASYNC_TESTS_TIMEOUT, \
    make_logger_mock, make_mock_channel_with


running_app_lists = [
    ['app0', 'app1'],
    ['app1', 'app2', 'app3'],
    ['app3', 'app2', 'app1', 'app0'],
    ['zooloo1']
]

state_input = [
    (
        dict(
            app1=(1, 'TestProfile1'),
            app2=(2, 'TestProfile2'),
            app3=(3, 'TestProfile1'),
            app4=(4, 'TestProfile2'),
            app5=(5, 'TestProfile1'),
        ),
        ['app1', 'app2'],
        0,
    ),
    (
        dict(
            app1=(1, 'TestProfile1'),
        ),
        ['app2'],
        1,
    ),
    (
        dict(
            app6=(1, 'TestProfile1'),
        ),
        ['app6'],
        1,
    ),
    (dict(), [], 0)
]


@pytest.fixture
def disp(mocker):
    node = mocker.Mock()
    node.list = mocker.Mock(
        return_value=make_mock_channel_with(['a1', 'a2', 'a3']))

    return burlak.StateAggregator(
        node,
        burlak.LoggerSetup(make_logger_mock(mocker), False),
        queues.Queue(), queues.Queue(), queues.Queue(),
        0.01)


@pytest.fixture
def init_state():
    return burlak.StateUpdateMessage(
        dict(
            app1=(10, 'TestProfile1'),
            app4=(5, 'TestProfile2'),
        ),
        1
    )


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_state_input(disp, mocker):
    stop_side_effect = [True for _ in state_input]
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    assert state_input

    for state, running_list, version in state_input:
        yield disp.input_queue.put(
            burlak.StateUpdateMessage(state, version))

    # disp.node_service.
    # yield disp.process_loop()

    for state, running_list, version in state_input:
        if not state:
            continue

        # command = yield disp.control_queue.get()
        # state_apps = set(state.iterkeys())

        # running_list_set = set(running_list)

        # assert command.to_stop == running_list_set - state_apps
        # assert command.to_run == state_apps - running_list_set
        # assert command.state == state
