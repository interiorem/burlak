from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop

@gen.coroutine
def uuid(service):
    ch = yield service.uuid()
    data = yield ch.rx.get()

    print 'data {}'.format(data)

    raise gen.Return(data)


@gen.coroutine
def extra(service):
    ch = yield service.extra()
    data = yield ch.rx.get()

    print 'data {}'.format(data)

    raise gen.Return(data)

uniresis = Service('uniresis')
IOLoop.current().run_sync(lambda : uuid(uniresis))
IOLoop.current().run_sync(lambda : extra(uniresis))
