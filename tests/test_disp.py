#
# TODO: more test for StateUpdateMessage
#
from cocaine.burlak import burlak
from cocaine.burlak.comm_state import CommittedState
from cocaine.burlak.context import Context, LoggerSetup
from cocaine.burlak.control_filter import ControlFilter

import pytest

from tornado import gen
from tornado import queues

from .common import ASYNC_TESTS_TIMEOUT, \
    make_future, make_logger_mock, make_mock_channel_with
from .common import MockSemaphore


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
    config.white_list = []

    sentry_wrapper = mocker.Mock()
    workers_distribution = dict()

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
        queues.Queue(), queues.Queue(), queues.Queue(), queues.Queue(),
        0.01,
        workers_distribution)


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

    running_apps_list = []
    for state, running_list, version in state_input:
        yield disp.input_queue.put(
            burlak.StateUpdateMessage(state, version, uuid=''))
        running_apps_list.append(running_list)

    control_filter = dict(apply_control=True, white_list=[])
    control_filter = ControlFilter.from_dict(control_filter)

    yield disp.filter_queue.put(burlak.ControlFilterMessage(control_filter))
    yield disp.input_queue.put(burlak.ResetStateMessage())

    def info_mock(app, flags=None):
        info = {
            app: {
                'pool': dict(
                    active=1,
                    idle=1,
                    slaves=dict(a=1, b=2, c=3),
                ),
            },
        }
        return make_mock_channel_with(info)

    disp.node_service.list = mocker.Mock(
        return_value=make_mock_channel_with(*running_apps_list))
    disp.node_service.info = mocker.Mock(side_effect=info_mock)

    yield disp.process_loop(MockSemaphore())

    msg = yield disp.control_queue.get()
    disp.control_queue.task_done()

    assert msg.control_filter.apply_control
    assert msg.control_filter.white_list == []

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


running_apps = [
    dict(app1=3, app2=2, app3=1, app4=4),
    dict(app1=3),
    dict(),
]


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_app_poll(disp, mocker):
    stop_side_effect = [True for _ in running_apps]
    stop_side_effect.append(True)  # reset state message
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    disp.input_queue.get = mocker.Mock(
        side_effect=[
            make_future(gen.TimeoutError()) for _ in running_apps
        ]
    )

    disp.node_service.list = mocker.Mock(
        side_effect=[
            make_mock_channel_with(d.keys()) for d in running_apps
        ]
    )

    def slaves_count(app):
        for apps in running_apps:
            for a in apps:
                if a == app:
                    return apps[a]

    def info_mock(app, flags=None):
        count = slaves_count(app)
        ans = dict(
            pool=dict(
                slaves={app: 'dummy_info' for _ in xrange(count)}
            )
        )

        return make_mock_channel_with(ans)

    def check_workers_mismatch(state, workers_count):
        for d in running_apps:
            if d == workers_count:
                return True
        return False

    disp.node_service.info = mocker.Mock(side_effect=info_mock)
    disp.workers_diff = mocker.Mock(side_effect=check_workers_mismatch)

    control_filter = dict(apply_control=True, white_list=[])
    control_filter = ControlFilter.from_dict(control_filter)

    yield disp.filter_queue.put(burlak.ControlFilterMessage(control_filter))

    yield disp.process_loop(MockSemaphore())

    assert disp.workers_diff.call_count == len(running_apps)

    for d in running_apps:
        assert disp.workers_diff.called_with(dict(), d)
