'''Dummy state updates generator

Use within functional tests.

TODO: verify send state by /state handlers

'''
import json
import random
import time

from math import ceil, cos, sin

import click

from cocaine.services import Service

from tornado import gen, httpclient
from tornado.ioloop import IOLoop

import yaml

from ..config import Config
from ..helpers.secadaptor import SecureServiceFabric
from ..uniresis import catchup_an_uniresis


UNICORN_STATE_PREFIX = '/state'
DEFAULT_SLEEP_TO_SEC = 4
DEFAULT_SLEEP_ON_ERROR_SEC = 10

X_INC = 0.05
AMPF = 20
DEFAULT_DISABLE_PROPORTION = 0.0

DEFAULT_PROFILE1 = 'IsoProcess'
DEFAULT_PROFILE2 = 'IsoProcess2'


def sample_sin(a, x):
    return a * sin(x) * sin(x)


def sample_cos(a, x):
    return a * cos(x) * cos(x)


def verify_state(input_state, result_state):
    for app, val in input_state.iteritems():
        orca_state = result_state[app]

        errors = []
        wrk = val['workers']
        if orca_state['workers'] != wrk:
            errors.append(
                'wrong number of workers for app {}, input {}, remote {}'
                .format(app, wrk, orca_state['workers']))

        prof = val['profile']
        if orca_state['profile'] != prof:
            errors.append(
                'wrong profile for app {}, input {}, remote {}'
                .format(app, prof, orca_state['profile']))

    if errors:
        raise Exception(errors)
    else:
        print('state verified at {}'.format(int(time.time())))


def make_state_path(prefix, uuid):
    return '{}/{}'.format(prefix, uuid)


@gen.coroutine
def state_pusher(
        unicorn, path_prefix, uniresis_stub_uuid, working_state,
        max_workers, to_sleep, verify_url, stop_proportion):

    uniresis = catchup_an_uniresis(uniresis_stub_uuid)

    uuid = yield uniresis.uuid()
    path = make_state_path(path_prefix, uuid)

    click.secho('pushing load to path {}'.format(path), fg='green')

    ch = yield unicorn.get(path)
    _, version = yield ch.rx.get()

    print 'version is {}'.format(version)

    if version == -1:
        yield unicorn.create(path, {})
        version = 0

    x = 0
    wrk_generators = [sample_sin, sample_cos]
    while True:
        try:
            x += X_INC

            state = {
                app: {
                    'workers': int(wrk_generators[
                        i % len(wrk_generators)](max_workers, x)) + 1,
                    'profile': random.choice(
                        [
                            prof
                            for (prof, weight) in working_state[app]
                            for p in xrange(int(weight))
                        ]
                    )
                }
                for i, (app, profiles) in enumerate(working_state.iteritems())
            }

            count_to_stop = int(ceil(stop_proportion * len(state)))

            to_stop = random.sample(
                state.keys(), min(count_to_stop, len(state)))
            for stop_app in to_stop:
                del state[stop_app]

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
            yield gen.sleep(DEFAULT_SLEEP_ON_ERROR_SEC)


@click.command()
@click.option(
    '--uuid-prefix',
    default=UNICORN_STATE_PREFIX,
    help='prefix path to store state in')
@click.option(
    '--uniresis-stub-uuid',
    help='use uniresis stub with specified uuid')
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
@click.option(
    '--proportion',
    default=DEFAULT_DISABLE_PROPORTION,
    help='randomly stop specified proportion of application'
)
def main(
        uuid_prefix, uniresis_stub_uuid, to_sleep, state_file, verify_url,
        max_workers, proportion):

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

    # TODO: not yet implemented (released actually) in framework!
    unicorn = SecureServiceFabric.make_secure_adaptor(
        Service('unicorn'), *config.secure, endpoints=config.locator_endpoints)

    IOLoop.current().run_sync(
        lambda:
            state_pusher(
                unicorn, uuid_prefix, uniresis_stub_uuid, emul_state,
                max_workers, to_sleep, verify_url, proportion))


if __name__ == '__main__':
    main()
