'''Uniresis service proxy

Used mostly for debug purposes as a stub, until real uniresis would
be available.

Remove candidate.
'''

from cocaine.services import Service

from tornado import gen


class ResoursesProxy(object):
    @gen.coroutine
    def uuid(self):  # pragma: no cover
        raise NotImplementedError(
            'uuid method sould be defined in derived classes')


class UniresisProxy(ResoursesProxy):

    def __init__(self, endpoints=None, name='uniresis'):
        self.uniresis = \
            Service(name, endpoints) if endpoints else Service(name)

    @gen.coroutine
    def uuid(self):
        ch = yield self.uniresis.uuid()
        uuid = yield ch.rx.get()
        raise gen.Return(uuid)


class DummyProxy():
    '''Stub for resource proxy
    Used for local tests purposes
    '''

    COCAINE_TEST_UUID = 'SOME_UUID'

    @gen.coroutine
    def uuid(self):
        raise gen.Return(DummyProxy.COCAINE_TEST_UUID)


def catchup_an_uniresis(use_stub=False, endpoints=None):
    if use_stub:
        return DummyProxy()
    else:
        return UniresisProxy(endpoints)
