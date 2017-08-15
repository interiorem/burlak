from cocaine.burlak import CommittedState

import pytest


all_runnung_state = dict(
    app1=('RUNNING', 1, 'a', 3, 2),
    app2=('RUNNING', 2, 'b', 2, 2),
    app3=('RUNNING', 3, 'c', 1, 2),
)

all_stopped_state = dict(
    app1=('STOPPED', 0, CommittedState.NA_PROFILE_LABEL, 1, 2),
    app2=('STOPPED', 0, CommittedState.NA_PROFILE_LABEL, 2, 2),
    app3=('STOPPED', 0, CommittedState.NA_PROFILE_LABEL, 3, 2),
)

mixed_state = dict(
    app1=('RUNNING', 1, 'a', 3, 2),
    app2=('STOPPED', 0, CommittedState.NA_PROFILE_LABEL, 1, 2),
    app3=('RUNNING', 3, 'b', 2, 2),
    app4=('STOPPED', 0, CommittedState.NA_PROFILE_LABEL, 2, 2),
    app5=('RUNNING', 3, 'c', 1, 5),
    app6=('STOPPED', 0, CommittedState.NA_PROFILE_LABEL, 3, 5),
)


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


def test_mixed_states(cstate):
    for k, (st, wrk, prof, ver, ts) in mixed_state.iteritems():
        if st == 'STOPPED':
            cstate.mark_stopped(k, ver, ts)
        elif st == 'RUNNING':
            cstate.mark_running(k, wrk, prof, ver, ts)

    assert cstate.as_dict() == mixed_state
