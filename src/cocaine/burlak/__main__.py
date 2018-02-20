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
from tornado.ioloop import IOLoop

from .comm_state import CommittedState
from .config import Config
from .context import Context, LoggerSetup
from .helpers import SecureServiceFabric
from .mokak.mokak import SharedStatus, make_status_web_handler
from .sentry import SentryClientWrapper
from .sys_metrics import SysMetricsGatherer
from .uniresis import catchup_an_uniresis
from .web import Uptime, WebOptions, make_web_app_v1


try:
    from .ver import __version__
    __version__ = str(__version__)
except ImportError:
    __version__ = 'unknown'


MODULE_NAME = 'cocaine.orca'


@click.command()
@click.option(
    '--uuid-prefix', help='state prefix (unicorn path)')
@click.option(
    '--apps-poll-interval', help='running apps list poll interval (seconds)')
@click.option('--port', type=int, help='web iface port')
@click.option(
    '--uniresis-stub-uuid', help='use uniresis stub with provided uuid')
@click.option(
    '--dup-to-console',
    is_flag=True, default=False, help='copy logger output to console')
@click.option(
    '--console-log-level',
    # See CocaineError.LEVELS for valid lavels numbers
    type=int, help='if console logger is active, set loglevel')
def main(
        uuid_prefix,
        apps_poll_interval,
        port,
        uniresis_stub_uuid,
        dup_to_console,
        console_log_level):

    committed_state = CommittedState()
    shared_status = SharedStatus(name=MODULE_NAME)

    config = Config(shared_status)
    config.update()

    if console_log_level is not None:
        config.console_log_level = console_log_level

    input_queue = queues.Queue(config.input_queue_size)
    white_list_queue = queues.Queue()
    control_queue = queues.Queue()

    logger = Logger(config.locator_endpoints)

    unicorn = SecureServiceFabric.make_secure_adaptor(
        Service(config.unicorn_name, config.locator_endpoints),
        *config.secure, endpoints=config.locator_endpoints)

    node = Service(config.node_name, config.locator_endpoints)

    uniresis = catchup_an_uniresis(
        uniresis_stub_uuid, config.locator_endpoints)

    sentry_wrapper = SentryClientWrapper(
        logger, dsn=config.sentry_dsn, revision=__version__)

    context = Context(
        LoggerSetup(logger, dup_to_console),
        config,
        __version__,
        sentry_wrapper,
        shared_status)

    if not apps_poll_interval:
        apps_poll_interval = config.apps_poll_interval_sec

    control_filter = burlak.ControlFilterListener(
        context, unicorn,
        white_list_queue, input_queue
    )

    acquirer = burlak.StateAcquirer(context, input_queue)
    workers_distribution = dict()
    state_processor = burlak.StateAggregator(
        context,
        node,
        committed_state,
        white_list_queue, input_queue, control_queue,
        apps_poll_interval,
        workers_distribution,
    )

    apps_elysium = burlak.AppsElysium(
        context, committed_state, node, control_queue)

    if not uuid_prefix:
        uuid_prefix = config.uuid_path

    # run async poll tasks in date flow reverse order, from sink to source
    io_loop = IOLoop.current()
    io_loop.spawn_callback(control_filter.subscribe_to_control_filter)
    io_loop.spawn_callback(apps_elysium.blessing_road)
    io_loop.spawn_callback(state_processor.process_loop)

    # io_loop.spawn_callback(
    #     # TODO: make node list constructor parameter
    #     lambda: acquirer.poll_running_apps_list(node_list))
    io_loop.spawn_callback(
        lambda: acquirer.subscribe_to_state_updates(
            unicorn, uniresis, uuid_prefix))

    qs = dict(input=input_queue, control=control_queue)
    units = dict(
        state_acquisition=acquirer,
        state_dispatch=state_processor,
        elysium=apps_elysium)

    cfg_port, prefix = config.web_endpoint

    if not port:
        port = cfg_port

    metrics_gatherer = SysMetricsGatherer()
    io_loop.spawn_callback(metrics_gatherer.gather)

    try:
        uptime = Uptime()
        wopts = WebOptions(
            prefix, port, uptime, uniresis,
            committed_state, metrics_gatherer,
            qs, units,
            workers_distribution,
            context.config,
            __version__,
        )
        web_app = make_web_app_v1(wopts) # noqa F841
        status_app = make_status_web_handler( # noqa F841
            shared_status, config.status_web_path, config.status_port)

        click.secho('orca is starting...', fg='green')
        IOLoop.current().start()
    except Exception as e:
        click.secho('error while spawning service: {}'.format(e), fg='red')


if __name__ == '__main__':
    main()
