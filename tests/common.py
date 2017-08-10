'''Service mock object
Mostly compy-pasted from darkvoice/tests/common.py
'''
import mock
from tornado.concurrent import Future


def make_future(v):
    '''
    TODO: exceptions cases
    '''
    fut = Future()
    fut.set_result(v)
    return fut


def make_mock_channel_with(*v):
    return make_future(MockChannel(*v))


class MockChannel(object):
    def __init__(self, *values):
        print('len of values {}'.format(len(values)))
        self.rx = mock.Mock()
        self.rx.get = mock.Mock(side_effect=[make_future(v) for v in values])
