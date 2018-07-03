'''Service mock object
Mostly copy-pasted from darkvoice/tests/common.py
'''
import mock

from tornado import gen
from tornado.concurrent import Future

ASYNC_TESTS_TIMEOUT = 10


def make_future(v):
    '''
    TODO: exceptions cases
    '''
    fut = Future()
    if isinstance(v, Exception) or isinstance(v, gen.TimeoutError):
        fut.set_exception(v)
    else:
        fut.set_result(v)

    return fut


def make_mock_channel_with(*v):
    return make_future(MockChannel(*v))


def make_mock_channels_list_with(sequence):
    return [make_future(MockChannel(v)) for v in sequence]


def make_mock_control_channel_with(*v):
    return make_future(MockControlChannel(*v))


def make_mock_control_list_with(sequence):
    return [make_mock_control_channel_with([v]) for v in sequence]


def make_logger_mock(mocker):
    logger = mocker.Mock()

    logger.debug = mocker.Mock()
    logger.error = mocker.Mock()
    logger.info = mocker.Mock()
    logger.warn = mocker.Mock()

    return logger


class MockChannel(object):
    def __init__(self, *values):
        self.rx = mock.Mock()
        self.rx.get = mock.Mock(side_effect=[make_future(v) for v in values])

        self.tx = mock.Mock()

        self.tx.write = mock.Mock(return_value=make_future(0))
        self.tx.close = mock.Mock(return_value=make_future(0))


class MockControlChannel(object):
    def __init__(self, *values):
        self.rx = mock.Mock()
        self.rx.get = \
            mock.Mock(side_effect=[make_future(0) for v in values])

        self.tx = mock.Mock()

        self.tx.write = mock.Mock(
            return_value=make_mock_channels_list_with(*values))
        self.tx.close = mock.Mock(return_value=make_future(0))
