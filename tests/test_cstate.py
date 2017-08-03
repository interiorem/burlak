import pytest

all_runnung_state = dict(
    app1=('RUNNING', 1, 2),
    app2=('RUNNING', 2, 2),
    app3=('RUNNING', 3, 2),
)


@pytest.fixture
def cstate():
    from cocaine.burlak import CommittedState
    return CommittedState()


def test_started_states(cstate):
    for k, (st, wrk, ts) in all_runnung_state.iteritems():
        cstate.mark_running(k, wrk, ts)

    state = cstate.as_dict()
    assert state == all_runnung_state


def test_stop_states():
    pass


def test_mixed_states():
    pass
