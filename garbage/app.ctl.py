from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop

import math

def func(a, x):
    z = math.sin(x)
    return a * z * z

@gen.coroutine
def control_app(app, base_x):

    ch = yield app.control()

    cnt = 0
    for val in (int(func(5, base_x + i * 0.1) + 1) for i in xrange(32)):
        print 'sending {} val({}): {}'.format(app, cnt+1, val)
        yield ch.tx.write(val)
        yield gen.sleep(2)
        cnt += 1

    print 'sequence send'
    yield gen.sleep(3)
    yield ch.tx.close()


@gen.coroutine
def app_ctl():
    apps = [
        Service('Echo'),
        Service('ppn'),
        # Service('Echo1'),
        # Service('Echo2'),
        # Service('Echo3'),
        # Service('Echo4'),
        # Service('Echo5'),
    ]

    x = 0
    while True:
        yield [control_app(app, x + i) for i, app in enumerate(apps)]
        x += 0.02
        break


IOLoop.current().run_sync(app_ctl)
