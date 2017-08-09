import burlak

import click

from cocaine.logger import Logger
from cocaine.services import SecureServiceFabric, Service

from tornado import queues
from tornado import web
from tornado.ioloop import IOLoop

from .config import Config

from .web import MetricsHandler, StateHandler


APP_LIST_POLL_INTERVAL = 10
DEFAULT_RUN_PROFILE = 'IsoProcess'


@click.command()
@click.option(
    '--uuid-prefix', help='state prefix (unicorn path)')
@click.option(
    '--default-profile',
    default=DEFAULT_RUN_PROFILE, help='default profile for app running')
@click.option(
    '--apps-poll-interval',
    default=APP_LIST_POLL_INTERVAL, help='default profile for app running')
@click.option('--port', help='web iface port')
@click.option(
    '--uniresis-stub',
    is_flag=True, default=False, help='use stub for uniresis uuid')
def main(
        uuid_prefix,
        default_profile,
        apps_poll_interval,
        port,
        uniresis_stub):

    config = Config()
    config.update()

    input_queue = queues.Queue()
    adjust_queue = queues.Queue()
    stop_queue = queues.Queue()

    # TODO: names from config
    logging = Logger()
    node = Service('node')
    unicorn = SecureServiceFabric.make_secure_adaptor(
        Service('unicorn'), *config.secure)

    acquirer = burlak.StateAcquirer(
        logging, node, input_queue, apps_poll_interval, uniresis_stub)
    state_processor = burlak.StateAggregator(
        logging, input_queue, adjust_queue, stop_queue)

    committed_state = burlak.CommittedState()

    apps_slayer = burlak.AppsSlayer(logging, committed_state, node, stop_queue)
    apps_baptizer = burlak.AppsBaptizer(
        logging, committed_state, node, adjust_queue, default_profile)

    if not uuid_prefix:
        uuid_prefix = config.uuid_path

    # run async poll tasks in date flow reverse order, from sink to source
    io_loop = IOLoop.current()

    io_loop.spawn_callback(apps_slayer.topheth_road)
    io_loop.spawn_callback(apps_baptizer.blessing_road)

    io_loop.spawn_callback(state_processor.process_loop)

    io_loop.spawn_callback(acquirer.poll_running_apps_list)
    io_loop.spawn_callback(
        lambda: acquirer.subscribe_to_state_updates(unicorn, uuid_prefix))

    qs = dict(input=input_queue, adjust=adjust_queue, stop=stop_queue)
    units = dict(
        acquisition=acquirer,
        state=state_processor,
        slayer=apps_slayer,
        baptizer=apps_baptizer)

    cfg_port, prefix = config.endpoint

    if not port:
        port = cfg_port

    app = web.Application([
        (prefix + r'/state', StateHandler,
            dict(committed_state=committed_state)),
        (prefix + r'/metrics', MetricsHandler,
            dict(queues=qs, units=units))
    ])

    app.listen(port)
    click.secho('orca is starting...', fg='green')
    IOLoop.current().start()


if __name__ == '__main__':
    main()
