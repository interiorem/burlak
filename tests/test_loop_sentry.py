from cocaine.burlak.loop_sentry import LoopSentry

import pytest


@pytest.fixture
def loop_sentry():
    return LoopSentry()


def test_loop_hast(loop_sentry):
    assert loop_sentry.should_run()
    assert loop_sentry.should_run()
    assert loop_sentry.should_run()

    loop_sentry.halt()
    assert not loop_sentry.should_run()
