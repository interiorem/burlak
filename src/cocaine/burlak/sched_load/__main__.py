'''Dummy state updates generator

Use within functional tests.

TODO: verify send state by /state handlers

'''
import json
import random
import time

from math import cos, sin

import click

from cocaine.services import SecureServiceFabric, Service

from tornado import gen, httpclient
from tornado.ioloop import IOLoop

import yaml

from ..config import Config


UNICORN_STATE_PREFIX = '/state/SOME_UUID'
DEFAULT_SLEEP_TO_SEC = 4
X_INC = 0.05
AMPF = 20

DEFAULT_PROFILE1 = 'IsoProcess'
DEFAULT_PROFILE2 = 'IsoProcess2'


def sample_sin(a, x):
    return a * sin(x) * sin(x)


def sample_cos(a, x):
    return a * cos(x) * cos(x)


def verify_state(input_state, result_state):
    for app, (wrk, prof) in input_state.iteritems():
        orca_state = result_state[app]
        # TODO: check state
        if orca_state[1] != wrk:
            raise Exception(
                'wrong number of workers for app {}, input {}, remote {}'
                .format(app, wrk, orca_state[1]))
        if orca_state[2] != prof:
            raise Exception(
                'wrong profile for app {}, input {}, remote {}'
                .format(app, prof, orca_state[2]))

    print('state verified at {}'.format(time.time()))


@gen.coroutine
def send_state(
        unicorn, path, working_state, max_workers, to_sleep, verify_url):

    ch = yield unicorn.get(path)
    _, version = yield ch.rx.get()

    x = 0
    wrk_generators = [sample_sin, sample_cos]
    while True:
        try:
            x += X_INC

            state = {
                app: (
                    int(wrk_generators[
                        i % len(wrk_generators)](max_workers, x)),
                    random.choice(
                        [
                            prof
                            for (prof, weight) in working_state[app]
                            for p in xrange(int(weight))
                        ]
                    )
                )
                for i, (app, profiles) in enumerate(working_state.iteritems())
            }

            ch = yield unicorn.put(path, state, version)
            _, (result, _) = yield ch.rx.get()

            version += 1
            print('send state: {}'.format(result))

            yield gen.sleep(to_sleep)

            if verify_url:
                response = yield httpclient.AsyncHTTPClient().fetch(verify_url)
                print 'orca state: {}'.format(response.body)
                verify_state(state, json.loads(response.body))

        except Exception as e:
            print('error: {}'.format(e))


@click.command()
@click.option(
    '--uuid-path',
    default=UNICORN_STATE_PREFIX,
    help='path to store state in')
@click.option(
    '--to-sleep',
    default=DEFAULT_SLEEP_TO_SEC,
    help='to sleep between updates')
@click.option(
    '--state-file',
    help='file to load state from')
@click.option(
    '--verify-url',
    help='orchestrator url to get last committed state'
)
@click.option(
    '--max-workers',
    default=AMPF,
    help='upper limit of workers to spawn'
)
def main(uuid_path, to_sleep, state_file, verify_url, max_workers):
    config = Config()
    config.update()

    def load_state(fname):
        print('reading state for emulation from {}'.format(fname))
        with open(fname) as fl:
            return yaml.load(fl)

    emul_state = load_state(state_file) if state_file else dict(
        ppn=[(DEFAULT_PROFILE1, 100), ],
        Echo=[(DEFAULT_PROFILE1, 50), (DEFAULT_PROFILE2, 5)],
        EchoWeb=[(DEFAULT_PROFILE1, 50), (DEFAULT_PROFILE2, 10)],
    )

    # TODO: not implemented (released yet) in framework!
    unicorn = SecureServiceFabric.make_secure_adaptor(
        Service('unicorn'), *config.secure)

    IOLoop.current().run_sync(
        lambda:
            send_state(
                unicorn, uuid_path, emul_state, max_workers,
                to_sleep, verify_url))


if __name__ == '__main__':
    main()
