from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop

import random


DEFAULT_WORKERS = 2
DEFAULT_TO_SLEEP = 30

DEFAULT_SERVICE = 'echo.orig1'
DEFAULT_PROFILE = 'IsoProcess'


@gen.coroutine
def control_seq(node, name, workers, to_sleep):
    try:
        ch = yield node.start_app(name, DEFAULT_PROFILE)
        _ = yield ch.rx.get()
        print 'Startign app completed'
    except Exception:
        pass

    ch = yield node.control(name)

    while True:
        print 'Starting {} workers'.format(workers)
        try:
            _ = yield ch.tx.write(workers)
            yield gen.sleep(to_sleep)
            print 'Stopping workers'
            _ = yield ch.tx.write(0)

            to_sleep_after = 3 + random.random() * DEFAULT_TO_SLEEP
            # to_sleep_after = 20
            print 'Sleeping for {:.1f}s'.format(to_sleep_after)

            yield gen.sleep(to_sleep_after)
        except Exception as e:
            print 'exception: {}'.format(e)
            yield gen.sleep(5)

@gen.coroutine
def safe_tx_close(ch):
    try:
        yield ch.tx.close()
    except Exception as e:
        print 'error on close {}'.format(e)


@gen.coroutine
def reconnect():
    yield gen.sleep(10)

    while True:
        try:
            echo = Service(DEFAULT_SERVICE)
            ch = yield echo.enqueue('ping')

            for i in xrange(10):
                msg = 'boo{}'.format(i)
                yield ch.tx.write(msg)
                data = yield ch.rx.get()

        except Exception as e:
            print 'error1 {}'.format(e)
            yield gen.sleep(3)


@gen.coroutine
def load(echo):
    yield gen.sleep(5.0)

    cnt = 0
    ch = yield echo.enqueue('ping')
    while True:
        try:
            yield ch.tx.write('zooo.{}'.format(cnt))
            data = yield ch.rx.get(timeout=20)
        except Exception as e:
            print 'error2 [{}]'.format(e)
            yield safe_tx_close(ch)
            yield gen.sleep(3)
            ch = yield echo.enqueue('ping')
            continue

        # print 'result {}'.format(data)
        # yield gen.sleep(random.random())

        cnt += 1
        if cnt % 100 == 0:
            yield gen.sleep(0.5)


node = Service('node')
service = Service(DEFAULT_SERVICE)

IOLoop.current().spawn_callback(lambda: load(service))
#IOLoop.current().spawn_callback(lambda: control_seq(
#    node, DEFAULT_SERVICE, DEFAULT_WORKERS, DEFAULT_TO_SLEEP))
#IOLoop.current().spawn_callback(reconnect)


IOLoop.current().start()
