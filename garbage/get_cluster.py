from cocaine.services import Service

from tornado.ioloop import IOLoop
from tornado import gen

@gen.coroutine
def get_uuid_by(service):
    ch = yield service.uuid()
    uuid = yield ch.rx.get()

    raise gen.Return(uuid)


locator = Service('locator')
uniresis = Service('uniresis')

uuid_by_locator = IOLoop.current().run_sync(lambda: get_uuid_by(locator))
uuid_by_uniresis = IOLoop.current().run_sync(lambda: get_uuid_by(uniresis))

print 'by locator: {}'.format(uuid_by_locator)
print 'by uniresis: {}'.format(uuid_by_uniresis)
