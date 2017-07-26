import burlak

import click

from cocaine.logger import Logger
from cocaine.services import Service

from tornado import queues
from tornado import web
from tornado.ioloop import IOLoop

from .config import Config
from .sec.sec import SecureServiceFabric


APP_LIST_POLL_INTERVAL = 10
DEFAULT_UNICORN_PATH = '/state/prefix'
COCAINE_TEST_UUID = 'SOME_UUID'

DEFAULT_RUN_PROFILE = 'IsoProcess'
DEFAULT_ORCA_PORT = 8877


def make_state_path(prefix, uuid):
    return prefix + '/' + uuid


@click.command()
@click.option(
    '--uuid',
    default=make_state_path(DEFAULT_UNICORN_PATH, COCAINE_TEST_UUID),
    help='runtime uuid (with unicorn path)')
@click.option(
    '--default-profile',
    default=DEFAULT_RUN_PROFILE, help='default profile for app running')
@click.option(
    '--apps-poll-interval',
    default=APP_LIST_POLL_INTERVAL, help='default profile for app running')
@click.option(
    '--port',
    default=DEFAULT_ORCA_PORT, help='web iface port')
def main(uuid, default_profile, apps_poll_interval, port):

    input_queue = queues.Queue()
    adjust_queue = queues.Queue()
    stop_queue = queues.Queue()

    config = Config()
    config.update()

    # TODO: names from config
    logging = Logger()
    node = Service('node')
    unicorn = SecureServiceFabric.make_secure_adaptor(
        Service('unicorn'), *config.secure)

    acquirer = burlak.StateAcquirer(
        logging, node, input_queue, uuid, apps_poll_interval)
    state_processor = burlak.StateAggregator(
        logging, input_queue, adjust_queue, stop_queue)

    committed_state = burlak.CommittedState()

    apps_slayer = burlak.AppsSlayer(logging, committed_state, node, stop_queue)
    apps_baptizer = burlak.AppsBaptizer(
        logging, committed_state, node, adjust_queue, default_profile)

    # run async poll tasks in date flow reverse order, from sink to source
    IOLoop.current().spawn_callback(lambda: apps_slayer.topheth_road())
    IOLoop.current().spawn_callback(lambda: apps_baptizer.blessing_road())

    IOLoop.current().spawn_callback(lambda: state_processor.process_loop())

    IOLoop.current().spawn_callback(lambda: acquirer.poll_running_apps_list())
    IOLoop.current().spawn_callback(
        lambda: acquirer.subscribe_to_state_updates(unicorn))

    qs = dict(input=input_queue, adjust=adjust_queue, stop=stop_queue)
    units = dict(
        acquisition=acquirer,
        state=state_processor,
        slayer=apps_slayer,
        baptizer=apps_baptizer)

    app = web.Application([
        (r'/state', burlak.StateHandler,
            dict(committed_state=committed_state)),
        (r'/metrics', burlak.MetricsHandler, dict(queues=qs, units=units))
    ])

    app.listen(port)
    IOLoop.current().start()


if __name__ == '__main__':
    main()
