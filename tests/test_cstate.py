from collections import OrderedDict

from cocaine.burlak.comm_state import CommittedState

import pytest


all_runnung_state = dict(
    app1=('STARTED', 1, 'a', 3, 2),
    app2=('STARTED', 2, 'b', 2, 2),
    app3=('STARTED', 3, 'c', 1, 2),
)

all_stopped_state = dict(
    app1=('STOPPED', 0, CommittedState.NA_PROFILE_LABEL, 1, 2),
    app2=('STOPPED', 0, CommittedState.NA_PROFILE_LABEL, 2, 2),
    app3=('STOPPED', 0, CommittedState.NA_PROFILE_LABEL, 3, 2),
)

mixed_state = dict(
    app1=('STARTED', 1, 'a', 3, 2),
    app2=('STOPPED', 0, CommittedState.NA_PROFILE_LABEL, 1, 20),
    app3=('STARTED', 3, 'b', 2, 2),
    app4=('STOPPED', 0, CommittedState.NA_PROFILE_LABEL, 2, 2),
    app5=('STARTED', 3, 'c', 1, 5),
    app6=('STOPPED', 0, CommittedState.NA_PROFILE_LABEL, 3, 50),
)


@pytest.fixture
def init_state(cstate):
    for k, (st, wrk, prof, ver, ts) in mixed_state.iteritems():
        if st == 'STOPPED':
            cstate.mark_stopped(k, ver, ts)
        elif st == 'STARTED':
            cstate.mark_running(k, wrk, prof, ver, ts)

    return cstate


@pytest.fixture
def cstate():
    return CommittedState()


def test_started_states(cstate):
    for k, (st, wrk, prof, ver, ts) in all_runnung_state.iteritems():
        cstate.mark_running(k, wrk, prof, ver, ts)

    assert cstate.as_dict() == all_runnung_state


def test_stop_states(cstate):
    for k, (_, wrk, _, ver, ts) in all_stopped_state.iteritems():
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
                    ('time_stamp', state[4]),
                ]) for app, state in mix.iteritems()
        }


@pytest.mark.parametrize(
    'app, profile, version, tm',
    [
        ('app2', 'some1', 1, 2),
        ('app3', 'some4', 3, 4),
    ])
def test_marke_failed(init_state, app, profile, version, tm):
    init_state.mark_failed(app, profile, version, tm)
    assert init_state.as_named_dict()[app] == \
        OrderedDict([
            ('state', 'FAILED'),
            ('workers', 0),
            ('profile', profile),
            ('state_version', version),
            ('time_stamp', tm),
        ])
