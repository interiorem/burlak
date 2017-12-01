#
# TODO: more test for StateUpdateMessage
#
from cocaine.burlak import burlak
from cocaine.burlak.comm_state import CommittedState
from cocaine.burlak.context import Context, LoggerSetup

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
            app1=dict(workers=1, profile='TestProfile1'),
            app2=dict(workers=2, profile='TestProfile2'),
            app3=dict(workers=3, profile='TestProfile1'),
            app4=dict(workers=4, profile='TestProfile2'),
            app5=dict(workers=5, profile='TestProfile1'),
        ),
        ['app1', 'app2'],
        0,
    ),
    (
        dict(
            app1=dict(workers=1, profile='TestProfile1'),
        ),
        ['app2'],
        1,
    ),
    (
        dict(
            app6=dict(workers=1, profile='TestProfile1'),
        ),
        ['app6'],
        1,
    ),
    (dict(), ['app7'], 0)
]


@pytest.fixture
def disp(mocker):
    node = mocker.Mock()
    node.list = mocker.Mock()
    config = mocker.Mock()

    sentry_wrapper = mocker.Mock()

    return burlak.StateAggregator(
        Context(
            LoggerSetup(make_logger_mock(mocker), False),
            config,
            '0',
            sentry_wrapper,
            mocker.Mock(),
        ),
        node,
        CommittedState(),
        queues.Queue(), queues.Queue(),
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
    stop_side_effect.append(True)  # reset state message

    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    assert state_input

    disp.sync_queue = mocker.Mock()
    disp.sync_queue.get = mocker.Mock(
        side_effect=[make_mock_channel_with(True) for _ in state_input])

    running_apps_list = []
    for state, running_list, version in state_input:
        yield disp.input_queue.put(
            burlak.StateUpdateMessage(state, version, uuid=''))
        running_apps_list.append(running_list)

    yield disp.input_queue.put(burlak.ResetCStateMessage())

    disp.node_service.list = mocker.Mock(
        return_value=make_mock_channel_with(*running_apps_list))

    yield disp.process_loop()

    for state, running_list, version in state_input:
        if not state:
            continue

        command = yield disp.control_queue.get()
        disp.control_queue.task_done()

        state_apps = set(state.iterkeys())
        running_list_set = set(running_list)

        assert command.to_stop == running_list_set - state_apps
        assert command.to_run == state_apps - running_list_set

        normalized_state = {
            app: burlak.StateRecord(val['workers'], val['profile'])
            for app, val in state.iteritems()
        }

        assert command.state == normalized_state
