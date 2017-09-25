#
# TODO:
#
# DONE:
#   - endpoints for logger
#
import burlak

import click

from cocaine.logger import Logger
# TODO: not released yet!
# from cocaine.services import SecureServiceFabric, Service
from cocaine.services import Service

from tornado import queues
from tornado import web
from tornado.ioloop import IOLoop

from .config import Config
from .context import Context, LoggerSetup
from .helpers import SecureServiceFabric

from .uniresis import catchup_an_uniresis
from .web import MetricsHandler, SelfUUID, StateHandler, Uptime


APP_LIST_POLL_INTERVAL = 10


@click.command()
@click.option(
    '--uuid-prefix', help='state prefix (unicorn path)')
@click.option(
    '--apps-poll-interval',
    default=APP_LIST_POLL_INTERVAL, help='default profile for app running')
@click.option('--port', help='web iface port')
@click.option(
    '--uniresis-stub-uuid', help='use uniresis stub with provided uuid')
@click.option(
    '--dup-to-console',
    is_flag=True, default=False, help='copy logger output to console')
def main(
        uuid_prefix,
        apps_poll_interval,
        port,
        uniresis_stub_uuid,
        dup_to_console):

    config = Config()
    config.update()

    input_queue, control_queue, sync_queue = \
        (queues.Queue() for _ in xrange(3))
    logging = Logger(config.locator_endpoints)

    unicorn = SecureServiceFabric.make_secure_adaptor(
        Service(config.unicorn_name, config.locator_endpoints),
        *config.secure, endpoints=config.locator_endpoints)

    node = Service(config.node_name, config.locator_endpoints)
    node_ctl = Service(config.node_name, config.locator_endpoints)

    uniresis = catchup_an_uniresis(
        uniresis_stub_uuid, config.locator_endpoints)

    context = Context(
        LoggerSetup(logging, dup_to_console), config)

    acquirer = burlak.StateAcquirer(
        context, input_queue)
    state_processor = burlak.StateAggregator(
        context,
        node,
        input_queue, control_queue, sync_queue,
        apps_poll_interval)

    committed_state = burlak.CommittedState()

    apps_elysium = burlak.AppsElysium(
        context, committed_state,
        node, node_ctl,
        control_queue, sync_queue)

    if not uuid_prefix:
        uuid_prefix = config.uuid_path

    # run async poll tasks in date flow reverse order, from sink to source
    io_loop = IOLoop.current()
    io_loop.spawn_callback(apps_elysium.blessing_road)
    io_loop.spawn_callback(state_processor.process_loop)

    # io_loop.spawn_callback(
    #     # TODO: make node list constructor parameter
    #     lambda: acquirer.poll_running_apps_list(node_list))
    io_loop.spawn_callback(
        lambda: acquirer.subscribe_to_state_updates(
            unicorn, node, uniresis, uuid_prefix))

    qs = dict(input=input_queue, control=control_queue, sync=sync_queue)
    units = dict(
        acquisition=acquirer,
        state=state_processor,
        elysium=apps_elysium)

    cfg_port, prefix = config.web_endpoint

    if not port:
        port = cfg_port

    uptime = Uptime()

    app = web.Application([
        (prefix + r'/state', StateHandler,
            dict(committed_state=committed_state)),
        (prefix + r'/metrics', MetricsHandler,
            dict(queues=qs, units=units)),
        # Used for testing/debugging, not for production, even could
        # cause problem if suspicious code will know node uuid.
        (prefix + r'/info', SelfUUID,
            dict(uniresis_proxy=uniresis, uptime=uptime)),
    ])

    app.listen(port)
    click.secho('orca is starting...', fg='green')
    IOLoop.current().start()


if __name__ == '__main__':
    main()
