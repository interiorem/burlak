from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop

UNICORN_STATE_PREFIX = '/state/prefix/SOME_UUID'

@gen.coroutine
def send_state(path):
    unicorn = Service('unicorn')

    wrk = 0
    while True:
        try:
            wrk += 1

            state = dict(Echo=wrk % 100)

            ch = yield unicorn.get(path)
            get_state, version = yield ch.rx.get()

            ch = yield unicorn.put(path, state, version)
            _, (result, _) = yield ch.rx.get()

            print('send state: {}'.format(result))

            yield gen.sleep(5)

        except Exception as e:
            print('error {}'.format(e))
            return

IOLoop.current().run_sync(lambda: send_state(UNICORN_STATE_PREFIX))
