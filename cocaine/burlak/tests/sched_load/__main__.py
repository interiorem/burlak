from math import cos, sin

import click

from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop

from ...config import Config
from ...sec.sec import SecureServiceFabric


UNICORN_STATE_PREFIX = '/state/SOME_UUID'
DEFAULT_SLEEP_TO_SEC = 3
X_INC = 0.05
AMPF = 50


def sample_sin(a, x):
    return a * sin(x) * sin(x)


def sample_cos(a, x):
    return a * cos(x) * cos(x)


@gen.coroutine
def send_state(unicorn, path, to_sleep):

    ch = yield unicorn.get(path)
    _, version = yield ch.rx.get()

    x = 0
    while True:
        try:
            x += X_INC

            state = dict(
                Echo=int(sample_sin(AMPF, x)),
                ppn=int(sample_cos(AMPF, x)))

            ch = yield unicorn.put(path, state, version)
            _, (result, _) = yield ch.rx.get()

            version += 1
            print('send state: {}'.format(result))

            yield gen.sleep(to_sleep)

        except Exception as e:
            print('error {}'.format(e))
            return


@click.command()
@click.option(
    '--uuid-path',
    default=UNICORN_STATE_PREFIX,
    help='path to store state in')
@click.option(
    '--to-sleep',
    default=DEFAULT_SLEEP_TO_SEC,
    help='to sleep between updates')
def main(uuid_path, to_sleep):
    config = Config()
    config.update()

    unicorn = SecureServiceFabric.make_secure_adaptor(
        Service('unicorn'), *config.secure)
    IOLoop.current().run_sync(lambda: send_state(unicorn, uuid_path, to_sleep))


if __name__ == '__main__':
    main()
