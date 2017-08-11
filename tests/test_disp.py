from cocaine.burlak import burlak

import pytest

from tornado import queues

from .common import ASYNC_TESTS_TIMEOUT, make_logger_mock


running_app_lists = [
    ['app0', 'app1'],
    ['app1', 'app2', 'app3'],
    ['app3', 'app2', 'app1', 'app0'],
    ['zooloo1']
]

state_input = [
    (dict(
        app1=(1, 'TestProfile1'),
        app2=(2, 'TestProfile2'),
        app3=(3, 'TestProfile1'),
        app4=(4, 'TestProfile2'),
        app5=(5, 'TestProfile1'),
    ), 0),
    (dict(
        app1=(1, 'TestProfile1'),
    ), 1),
    (dict(
        app6=(1, 'TestProfile1'),
    ), 1),
    (dict(), 0)
]


@pytest.fixture
def disp(mocker):
    return burlak.StateAggregator(
        make_logger_mock(mocker), queues.Queue(), queues.Queue())


@pytest.fixture
def init_state():
    return burlak.StateUpdateMessage(
        dict(
            app1=(10, 'TestProfile1'),
            app4=(5, 'TestProfile2'),
        ), 1
    )


@pytest.fixture
def init_running_state():
    return burlak.RunningAppsMessage(['app' + str(i) for i in xrange(0, 4)])


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_apps_list_input(disp, mocker, init_state):
    stop_side_effect = [True for _ in running_app_lists]
    stop_side_effect.append(True)  # should be init state
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    yield disp.input_queue.put(init_state)

    assert running_app_lists

    for apps_list in running_app_lists:
        yield disp.input_queue.put(burlak.RunningAppsMessage(apps_list))

    yield disp.process_loop()

    init_running_set = set(init_state.get_state().iterkeys())
    init_command = yield disp.control_queue.get()

    assert init_running_set == init_command.to_run
    assert not init_command.to_stop

    disp.control_queue.task_done()

    for apps_list in running_app_lists:

        command = yield disp.control_queue.get()

        assert command.to_run == init_running_set - set(apps_list)
        assert command.to_stop == set(apps_list) - init_running_set

        disp.control_queue.task_done()


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_state_input(disp, mocker, init_running_state):
    stop_side_effect = [True for _ in state_input]
    stop_side_effect.append(True)  # init running apps state
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    assert state_input

    yield disp.input_queue.put(init_running_state)
    for state, version in state_input:
        yield disp.input_queue.put(burlak.StateUpdateMessage(state, version))

    yield disp.process_loop()

    for state, version in state_input:
        if not state:
            continue

        command = yield disp.control_queue.get()
        state_apps = set(state.iterkeys())

        init_state = init_running_state.get_apps_set()

        assert command.to_stop == init_state - state_apps
        assert command.to_run == state_apps - init_state
