# TODO: app control tests
from cocaine.burlak import burlak
from cocaine.burlak.chcache import ChannelsCache, _AppsCache
from cocaine.burlak.comm_state import CommittedState
from cocaine.burlak.config import Config
from cocaine.burlak.context import Context, LoggerSetup

import pytest

from tornado import queues

from .common import ASYNC_TESTS_TIMEOUT, \
    make_logger_mock, make_mock_channel_with, make_mock_channels_list_with


to_stop_apps = [
    ['app3', 'app4', 'app5', 'app6', 'app7'],
    ['app2', 'app3'],
    ['app3'],
]

to_run_apps = [
    dict(run3=burlak.StateRecord(3, 't1')),
    dict(
        run2=burlak.StateRecord(2, 't2'),
        run3=burlak.StateRecord(3, 't3'),
        run4=burlak.StateRecord(4, 't4'),
    ),
    dict(
        run3=burlak.StateRecord(3, 't3'),
        run4=burlak.StateRecord(4, 't4'),
        run5=burlak.StateRecord(5, 't5'),
        run6=burlak.StateRecord(6, 't6'),
        run7=burlak.StateRecord(7, 't7'),
    ),
    dict(),
]


def count_apps(list_of_dict):
    return sum(map(len, (d for d in list_of_dict)))


@pytest.fixture
def elysium(mocker):

    config = Config(mocker.Mock())
    sentry_wrapper = mocker.Mock()

    mocker.patch.object(
        _AppsCache,
        'make_control_ch',
        return_value=make_mock_channel_with(0))

    node = mocker.Mock()
    node.start_app = mocker.Mock(
        side_effect=make_mock_channels_list_with(
            xrange(count_apps(to_run_apps))
        )
    )

    node.control = mocker.Mock(
        side_effect=make_mock_channels_list_with(
            xrange(count_apps(to_run_apps))
        )
    )

    return burlak.AppsElysium(
        Context(
            LoggerSetup(make_logger_mock(mocker), False),
            config,
            '0',
            sentry_wrapper,
            mocker.Mock(),
        ),
        CommittedState(),
        node,
        queues.Queue(), queues.Queue())


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_stop(elysium, mocker):
    stop_side_effect = [True for _ in to_stop_apps]
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    for stop_apps in to_stop_apps:
        yield elysium.control_queue.put(
            burlak.DispatchMessage(
                dict(), dict(),
                -1,
                False,
                set(stop_apps),
                set(),
                False,
                set(), set(),
            )
        )

    elysium.context.config._config['stop_apps'] = True
    elysium.context.config._config['stop_by_control'] = False

    elysium.node_service.pause_app = mocker.Mock(
        side_effect=make_mock_channels_list_with(
            xrange(count_apps(to_stop_apps))
        )
    )

    yield elysium.blessing_road()

    for apps_list in to_stop_apps:
        for app in apps_list:
            assert elysium.node_service.pause_app.called_with(app)

    assert elysium.node_service.pause_app.call_count == \
        sum(map(len, to_stop_apps))


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_stop_by_control(elysium, mocker):
    stop_side_effect = [True for _ in to_stop_apps]
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    for stop_apps in to_stop_apps:
        yield elysium.control_queue.put(
            burlak.DispatchMessage(
                dict(), dict(),
                -1, False,
                set(stop_apps), set(),
                False,
                set(), set(),
            )
        )

    elysium.context.config._config['stop_apps'] = True
    elysium.context.config._config['stop_by_control'] = True

    mocker.patch.object(
        ChannelsCache,
        'get_ch',
        return_value=make_mock_channel_with(0)
    )

    yield elysium.blessing_road()

    for apps_list in to_stop_apps:
        for app in apps_list:
            assert ChannelsCache.get_ch.called_with(app)

    assert \
        ChannelsCache.get_ch.call_count == \
        len({a for apps in to_stop_apps for a in apps})


@pytest.mark.parametrize(
    'log_pending_stop',
    [
        True,
        False
    ]
)
@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_stop_apps_disabled(elysium, mocker, log_pending_stop):
    stop_side_effect = [True for _ in to_stop_apps]
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    elysium.context.config.pending_stop_in_state = log_pending_stop

    for stop_apps in to_stop_apps:
        yield elysium.control_queue.put(
            burlak.DispatchMessage(
                dict(), dict(),
                -1, False,
                set(stop_apps), set(),
                False,
                set(), set(),
            )
        )

    elysium.node_service.pause_app = mocker.Mock(
        side_effect=make_mock_channels_list_with(
            xrange(count_apps(to_stop_apps))
        )
    )

    yield elysium.blessing_road()
    assert elysium.node_service.pause_app.call_count == 0


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_run(elysium, mocker):
    stop_side_effect = [True for _ in to_run_apps]
    stop_side_effect.append(False)
    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    for run_apps in to_run_apps:
        yield elysium.control_queue.put(
            burlak.DispatchMessage(
                run_apps,
                dict(),
                -1,
                False,
                set(),
                set(run_apps.iterkeys()),
                False,
                set(),
                set(),
            )
        )

    mocker.patch.object(
        ChannelsCache,
        'get_ch',
        return_value=make_mock_channel_with(0)
    )

    yield elysium.blessing_road()

    for apps_list in to_run_apps:
        for app, record in apps_list.iteritems():
            assert elysium.node_service.start_app.called_with(
                app,
                record.profile
            )

    assert \
        elysium.node_service.start_app.call_count == \
        count_apps(to_run_apps)


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_control(elysium, mocker):
    stop_side_effect = [True for _ in to_run_apps]
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    for run_apps in to_run_apps:
        yield elysium.control_queue.put(
            burlak.DispatchMessage(
                run_apps,
                dict(),
                -1,
                True,
                set(),
                set(run_apps.iterkeys()),
                False,
                set(),
                set(),
            )
        )

    mocker.patch.object(
        ChannelsCache,
        'get_ch',
        return_value=make_mock_channel_with(0)
    )

    yield elysium.blessing_road()

    for apps_list in to_run_apps:
        for app, record in apps_list.iteritems():
            assert elysium.node_service.start_app.called_with(
                app,
                record.profile
            )

            assert _AppsCache.make_control_ch.called_with(app)

    assert \
        elysium.node_service.start_app.call_count == \
        count_apps(to_run_apps)

    assert \
        ChannelsCache.get_ch.call_count == \
        len(list(app for task in to_run_apps for app in task))


#
# TODO: exceptions count!
#
@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_control_exceptions(elysium, mocker):
    stop_side_effect = [True for _ in to_run_apps]
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    for run_apps in to_run_apps:
        yield elysium.control_queue.put(
            burlak.DispatchMessage(
                run_apps,
                dict(),
                -1,
                True,
                set(),
                set(run_apps.iterkeys()),
                False,
                set(),
                set(),
            )
        )

    except_sequence = [
        0,
        Exception('broken', 'connect1'),
        Exception('broken', 'connect2'),
        0,
        Exception('broken', 'connect3'),
        0,
    ]

    except_count = 0
    for it in except_sequence:
        if isinstance(it, Exception):
            except_count += 1

    elysium.node_service.control = mocker.Mock(
        side_effect=[make_mock_channel_with(v) for v in except_sequence])
    yield elysium.blessing_road()

    for apps_list in to_run_apps:
        for app, record in apps_list.iteritems():
            assert elysium.node_service.start_app.called_with(
                app,
                record.profile
            )

            assert elysium.node_service.control.called_with(app)

    assert \
        elysium.node_service.start_app.call_count == \
        sum(map(len, to_run_apps))
    assert \
        elysium.node_service.control.call_count == \
        len(set(app for task in to_run_apps for app in task))


# TODO: redactor
@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def skipped_test_gapped_control(elysium, mocker):
    '''Test for malformed state and to_run list combination'''
    stop_side_effect = [True for _ in to_run_apps]
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)

    gapped_states = []
    trig = 0
    for state in to_run_apps:
        trig ^= 1

        if trig:
            keys_to_preserve = list(state.keys())[0:-1]
        else:
            keys_to_preserve = list(state.keys())[1:]

        gapped_states.append({
            k: v
            for k, v in state.iteritems()
            if k in keys_to_preserve})

    for state, gap_state in zip(to_run_apps, gapped_states):
        yield elysium.control_queue.put(
            burlak.DispatchMessage(
                gap_state,
                dict(),
                -1,
                True,
                set(),
                set(state.iterkeys()),
                False,
                set(),
                set(),
            )
        )

    yield elysium.blessing_road()

    for state in to_run_apps:
        for app, record in state.iteritems():
            assert elysium.node_service.start_app.called_with(
                app,
                record.profile
            )

    for state in gapped_states:
        for app, record in state.iteritems():
            assert _AppsCache.make_control_ch.called_with(app)

    assert elysium.node_service.start_app.call_count == \
        sum(map(len, gapped_states))

    assert _AppsCache.make_control_ch.call_count == \
        len(set(app for task in to_run_apps for app in task))
