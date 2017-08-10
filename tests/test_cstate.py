import pytest

all_runnung_state = dict(
    app1=('RUNNING', 1, 3, 2),
    app2=('RUNNING', 2, 2, 2),
    app3=('RUNNING', 3, 1, 2),
)

all_stopped_state = dict(
    app1=('STOPPED', 0, 1, 2),
    app2=('STOPPED', 0, 2, 2),
    app3=('STOPPED', 0, 3, 2),
)

mixed_state = dict(
    app1=('RUNNING', 1, 3, 2),
    app2=('STOPPED', 0, 1, 2),
    app3=('RUNNING', 3, 2, 2),
    app4=('STOPPED', 0, 2, 2),
    app5=('RUNNING', 3, 1, 2),
    app6=('STOPPED', 0, 3, 2),
)


@pytest.fixture
def cstate():
    from cocaine.burlak import CommittedState
    return CommittedState()


def test_started_states(cstate):
    for k, (st, wrk, ver, ts) in all_runnung_state.iteritems():
        cstate.mark_running(k, wrk, ver, ts)

    assert cstate.as_dict() == all_runnung_state


def test_stop_states(cstate):
    for k, (st, wrk, ver, ts) in all_stopped_state.iteritems():
        cstate.mark_stopped(k, ver, ts)

    assert cstate.as_dict() == all_stopped_state


def test_mixed_states(cstate):
    for k, (st, wrk, ver, ts) in mixed_state.iteritems():
        if st == 'STOPPED':
            cstate.mark_stopped(k, ver, ts)
        elif st == 'RUNNING':
            cstate.mark_running(k, wrk, ver, ts)

    assert cstate.as_dict() == mixed_state
