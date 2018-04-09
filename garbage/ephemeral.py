from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop


@gen.coroutine
def create(path):
    unicorn = Service('unicorn')

    print 'creating node {}'.format(path)

    ch = yield unicorn.create_with(path, {}, dict(ephemeral=True))
    # ch = yield unicorn.create(path, {})
    _ = yield ch.rx.get()

    print 'creating done'
    yield gen.sleep(10)

    print 'node should be removed {}'.format(path)

    yield ch.tx.close()


    # ch = yield unicorn.get(path)
    # _, version = yield ch.rx.get()

    # ch = yield unicorn.remove(path, version)
    # _ = yield ch.rx.get()


path = '/darkvoice/boomoo'
IOLoop.current().run_sync(lambda : create(path))
