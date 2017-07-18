import click

from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop

from math import sin, cos


UNICORN_STATE_PREFIX = '/state/prefix/SOME_UUID'
DEFAULT_SLEEP_TO_SEC = 3


def sample_sin(a, x):
    return a * sin(x) * sin(x)


def sample_cos(a, x):
    return a * cos(x) * cos(x)


@gen.coroutine
def send_state(path, to_sleep):
    unicorn = Service('unicorn', to_sleep)

    wrk = 0

    ch = yield unicorn.get(path)
    _, version = yield ch.rx.get()

    while True:
        try:
            wrk += 0.05

            state = dict(
                Echo=int(sample_sin(50, wrk)),
                ppn=int(sample_cos(50, wrk)))

            ch = yield unicorn.put(path, state, version)
            _, (result, _) = yield ch.rx.get()

            version += 1
            print('send state: {}'.format(result))

            yield gen.sleep(to_sleep)

        except Exception as e:
            print('error {}'.format(e))
            return


@click.command
@click.option(
    '--uuid-path',
    default=UNICORN_STATE_PREFIX,
    help='path to store state in')
@click.option(
    '--to-sleep',
    default=DEFAULT_SLEEP_TO_SEC
    help='to sleep between updates')
def main(uuid_path, to_sleep):
    IOLoop.current().run_sync(lambda: send_state(uuid_path, to_sleep))


if __name__ == '__main__':
    main()
