from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop

import math

def func(a, x):
    z = math.sin(x)
    return a * z * z

@gen.coroutine
def app_ctl():
    echo = Service('Echo1')

    x = 0
    while True:

        ch = yield echo.control()
        cnt = 0
        for val in (int(func(5, x + i * 0.1) + 1) for i in xrange(32)):
            print 'sending val({}): {}'.format(cnt+1, val)
            yield ch.tx.write(val)
            yield gen.sleep(3)
            cnt += 1

        print 'sequence send'
        yield gen.sleep(5)
        yield ch.tx.close()

        x += 0.02

    raise gen.Return(data)


res = IOLoop.current().run_sync(app_ctl)
print 'res {}'.format(res)
