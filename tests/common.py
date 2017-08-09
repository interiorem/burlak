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


class MockChannel(object):
    def __init__(self, *values):
        self.rx = mock.Mock()
        self.rx.get = mock.Mock(side_effect=[make_future(v) for v in values])
