from cocaine.burlak import SentryClientWrapper

import pytest

from .common import make_logger_mock


TEST_DSN = 'local.test.sentry.org'
TEST_REVISION = 'rev.test'

EXCEPTIONS_COUNT = 7

CAPTURED_MESSAGES = [
    'If you have something to say',
    "Don't keep it within",
    "Tell the World, shoud it make it's day?"
]


@pytest.fixture
def sentry_wrapper(mocker):
    mocker.patch('raven.Client', autospec=True)
    return SentryClientWrapper(
        make_logger_mock(mocker),
        TEST_DSN,
        TEST_REVISION
    )


def test_capture_exceptions(sentry_wrapper):
    for i in xrange(EXCEPTIONS_COUNT):
        sentry_wrapper.capture_exception()

    assert \
        sentry_wrapper.client.captureException.call_count == EXCEPTIONS_COUNT


def test_capture_message(sentry_wrapper, mocker):

    sentry_wrapper.client.captureMessage = mocker.Mock()

    for msg in CAPTURED_MESSAGES:
        sentry_wrapper.capture_message(msg)

    for msg in CAPTURED_MESSAGES:
        assert sentry_wrapper.client.captureMessage.called_with(msg)
