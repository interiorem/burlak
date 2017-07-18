from tornado import gen
from tornado.ioloop import IOLoop

from cocaine.services import Service

APP_NAME = 'Echo'
PROFILE = 'DefaultProfile'


@gen.coroutine
def control():
    node = Service('node')
    logger = Service('logging')

    try:
        ch = yield node.start_app(APP_NAME, PROFILE)
        result = yield ch.rx.get()

        print('start res {}'.format(result))
        yield logger.emit
    except Exception:
        pass

    control_channel = yield node.control(APP_NAME)
    for i in xrange(0, 5):
        print('running {}'.format(i))
        res = yield control_channel.tx.write(1)

    # yield control_channel.tx.close()

    # print('pausing app...')
    # ch = yield node.pause_app(APP_NAME)
    # result = yield ch.rx.get()
    #
    # print('pause app {}'.format(result))


IOLoop.current().run_sync(control)
