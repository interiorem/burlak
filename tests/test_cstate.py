from collections import OrderedDict

from cocaine.burlak.burlak import StateRecord, update_fake_state
from cocaine.burlak.comm_state import CommittedState, Defaults
from cocaine.burlak.control_filter import ControlFilter


import pytest


TEST_STATE_VERSION = 42


all_runnung_state = dict(
    app1=('STARTED', 1, 'a', 3, Defaults.SUCCESS_DESCRIPTION, 2),
    app2=('STARTED', 2, 'b', 2, Defaults.SUCCESS_DESCRIPTION, 2),
    app3=('STARTED', 3, 'c', 1, Defaults.SUCCESS_DESCRIPTION, 2),
)

all_stopped_state = dict(
    app1=(
        'STOPPED', 0,
        Defaults.NA_PROFILE_LABEL, 1, Defaults.STOPPED_DESCRIPTION, 2
    ),
    app2=(
        'STOPPED', 0,
        Defaults.NA_PROFILE_LABEL, 2, Defaults.STOPPED_DESCRIPTION, 2
    ),
    app3=(
        'STOPPED', 0,
        Defaults.NA_PROFILE_LABEL, 3, Defaults.STOPPED_DESCRIPTION, 2
    ),
)

mixed_state = dict(
    app1=(
        'STARTED', 1,
        'a', 3, Defaults.SUCCESS_DESCRIPTION, 2),
    app2=(
        'STOPPED', 0,
        Defaults.NA_PROFILE_LABEL, 1, Defaults.STOPPED_DESCRIPTION, 20),
    app3=(
        'STARTED', 3,
        'b', 2, Defaults.SUCCESS_DESCRIPTION, 2),
    app4=(
        'STOPPED', 0,
        Defaults.NA_PROFILE_LABEL, 2, Defaults.STOPPED_DESCRIPTION, 2),
    app5=(
        'STARTED', 3,
        'c', 1, Defaults.SUCCESS_DESCRIPTION, 5),
    app6=(
        'STOPPED', 0,
        Defaults.NA_PROFILE_LABEL, 3, Defaults.STOPPED_DESCRIPTION, 50),
)


test_cf = ControlFilter(False, ['x', 'y', 'z'])


@pytest.fixture
def init_state(cstate):
    for k, (st, wrk, prof, ver, _, ts) in mixed_state.iteritems():
        if st == 'STOPPED':
            cstate.mark_stopped(k, ver, ts)
        elif st == 'STARTED':
            cstate.mark_running(k, wrk, prof, ver, ts)

    return cstate


@pytest.fixture
def cstate():
    return CommittedState()


def test_started_states(cstate):
    for k, (st, wrk, prof, ver, _, ts) in all_runnung_state.iteritems():
        cstate.mark_running(k, wrk, prof, ver, ts)

    assert cstate.as_dict() == all_runnung_state


def test_stop_states(cstate):
    for k, (_, wrk, _, ver, _, ts) in all_stopped_state.iteritems():
        cstate.mark_stopped(k, ver, ts)

    assert cstate.as_dict() == all_stopped_state


def test_mixed_states(init_state):
    assert init_state.as_dict() == mixed_state


def test_expire_stopped(init_state, mocker):
    expire = 11
    now = 60

    mix = dict(mixed_state)

    mocker.patch('time.time', return_value=now)
    init_state.remove_old_stopped(expire)

    to_remove = [
        app for app, state in mix.iteritems()
        if state[0] == 'STOPPED' and state[-1] < now - expire
    ]

    assert len(to_remove) != 0

    for app in to_remove:
        del mix[app]

    assert init_state.as_dict() == mix
    assert init_state.as_named_dict() == \
        {
            app: OrderedDict([
                    ('state', state[0]),
                    ('workers', state[1]),
                    ('profile', state[2]),
                    ('state_version', state[3]),
                    ('state_description', state[4]),
                    ('time_stamp', state[5]),
                ]) for app, state in mix.iteritems()
        }


@pytest.mark.parametrize(
    'app, profile, version, tm',
    [
        ('app2', 'some1', 1, 2),
        ('app3', 'some4', 3, 4),
    ])
def test_mark_failed(init_state, app, profile, version, tm):
    init_state.mark_failed(app, profile, version, tm)

    assert init_state.as_named_dict()[app] == \
        OrderedDict([
            ('state', 'FAILED'),
            ('workers', 0),
            ('profile', profile),
            ('state_version', version),
            ('state_description', Defaults.FAILED_DESCRIPTION),
            ('time_stamp', tm),
        ])

    assert app in init_state.failed


def test_version(init_state):
    init_state.version = 100500
    assert init_state.version == 100500


def test_running_apps_count(init_state):
    assert init_state.running_apps_count() == \
        sum(
                1 for record in init_state.as_dict().itervalues()
                if record.state == 'STARTED'
        )


def test_workers_count(init_state):
    assert init_state.workers_count() == \
        sum(
            record.workers
            for record in init_state.as_dict().itervalues()
            if record.state == 'STARTED'
        )


def test_control_filter(init_state):
    init_state.control_filter = test_cf
    assert init_state.control_filter == test_cf


@pytest.mark.parametrize(
    'real_state,control_state',
    [
        (
            dict(
                app1=StateRecord(100500, 'prof1'),
                app2=StateRecord(100501, 'prof2'),
                app3=StateRecord(100502, 'prof3'),
                app4=StateRecord(100503, 'prof4'),
            ),
            dict(
                app3=1,
                app1=1
            ),
        ),
        (
            dict(
                app1=StateRecord(1, 'prof1'),
                app2=StateRecord(2, 'prof2'),
            ),
            dict(app3=1, app1=1),
        ),
        (
            dict(),
            dict(app1=1, app2=1),
        ),
        (
            dict(),
            dict(),
        ),
        (
            dict(
                app1=StateRecord(1, 'prof1'),
                app2=StateRecord(2, 'prof2'),
            ),
            dict(),
        )
    ]
)
def test_fake_state(init_state, real_state, control_state):

    init_state.clear()

    update_fake_state(
        init_state, TEST_STATE_VERSION, real_state, control_state)

    assert init_state.version == TEST_STATE_VERSION
    running_set = \
        {
            k for k, v in init_state.as_dict().iteritems()
            if v.state == 'STARTED'
        }
    marked_set = real_state.viewkeys() - control_state.viewkeys()

    assert running_set == marked_set
