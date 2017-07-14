#
# TODO:
#   - use coxx logger
#   - take start_app 'profile' from, emmm... state?
#   - secure service for 'unicorn', maybe for 'node', 'logger'?
#   - expose state to andle
#
import click

from tornado import gen
from tornado.ioloop import IOLoop
from tornado import web
from tornado import queues

from cocaine.services import Service
from cocaine.exceptions import ServiceError, DisconnectionError
from cocaine.logger import Logger

from collections import defaultdict

APP_LIST_POLL_INTERVAL = 5
DEFAULT_RETRY_TIMEOUT_SEC = 5

UNICORN_STATE_PREFIX = '/state/prefix'
COCAINE_TEST_UUID = 'SOME_UUID'
DEFAULT_RUN_PROFILE = 'DefaultProfile'
DEFAULT_ORCA_PORT = 8877

SELF_NAME = 'app/orca'

def make_state_path(uuid):
    return UNICORN_STATE_PREFIX + '/' + uuid


class MetricsHandler(web.RequestHandler):

    def initialize(self, queues, units):
        self.queues = queues
        self.units = units

    def get(self):
        metrics = dict(
            queues_fill={
                k: v.qsize() for k,v in self.queues.iteritems()
            },
            metrics={
                k: v.get_metrics() for k,v in self.units.iteritems()
            }
        )
        self.write(metrics)


class StateHandler(web.RequestHandler):
    def get(self):
        self.write('<h3>not implemented</h3>')


class RunningAppsMessage(object):
    def __init__(self, run_list=[]):
        self.running_apps = set(run_list)

    def get_apps_set(self):
        return self.running_apps


class StateUpdateMessage(object):
    def __init__(self, state={}):
        self.state = state

    def get_state(self):
        return self.state


class MetricsMixin(object):

    def __init__(self):
        self.metrics = defaultdict(int)

    def get_metrics(self):
        return self.metrics

class StateAcquirer(MetricsMixin):

    def __init__(self, logger, node, unicorn, input_queue, state_path,
                 poll_interval_sec=APP_LIST_POLL_INTERVAL):

        super(StateAcquirer, self).__init__()

        self.logger = logger

        self.node_service = node
        self.unicorn_service = unicorn
        self.input_queue = input_queue
        self.state_path = state_path
        self.poll_interval_sec = poll_interval_sec

    @gen.coroutine
    def poll_running_apps_list(self):
        while True:
            try:
                print('poll: getting apps list')

                ch = yield self.node_service.list()
                app_list = yield ch.rx.get()

                self.metrics['running_nodes'] = len(app_list)

                print('poll: got apps list {}'.format(app_list))

                yield self.input_queue.put(RunningAppsMessage(app_list))
                yield gen.sleep(self.poll_interval_sec)
            except Exception as e:
                print('failed to get app list, error: {}'.format(e))
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)

    @gen.coroutine
    def subscribe_to_state_updates(self):
        ch = yield self.unicorn_service.subscribe(self.state_path)

        while True:
            try:
                result, version = yield ch.rx.get()
                print('subscribe: got subscribed state {} {}'.format(result, version))
                yield self.input_queue.put(StateUpdateMessage(result))

                self.metrics['adjust_state_commands_count'] += len(result)
            except Exception as e:
                print(
                    'subscribe: failed to get unicorn subscription with {}'
                    .format(e))
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
                # TODO: remove previous subscription?
                ch = yield self.unicorn_service.subscribe(self.state_path)


class StateAggregator(MetricsMixin):

    def __init__(self, logger, input_queue, adjust_queue, stop_queue):
        super(StateAggregator, self).__init__()

        self.logger = logger

        self.input_queue = input_queue
        self.stop_queue = stop_queue
        self.adjust_queue = adjust_queue

        self.running_apps_set = set()
        self.state = dict()

    @gen.coroutine
    def process_loop(self):
        while True:
            msg = yield self.input_queue.get()

            is_state_updated = False

            try:
                if isinstance(msg, RunningAppsMessage):
                    self.running_apps_set = msg.get_apps_set()
                    print(
                        'disp: got running apps list {}'
                        .format(self.running_apps_set))
                elif isinstance(msg, StateUpdateMessage):
                    self.state = msg.get_state()
                    is_state_updated = True
                    print('disp: got state update {}'.format(self.state))
                else:
                    print('unknown message {}'.format(msg))
            except Exception as e:
                print('input queue read error {}'.format(e))
            finally:
                self.input_queue.task_done()

            update_state_apps_set = set(self.state.iterkeys())

            to_run = update_state_apps_set - self.running_apps_set
            to_stop = self.running_apps_set - update_state_apps_set

            print('to_run {}'.format(to_run))
            print('to_stop {}'.format(to_stop))

            if to_stop:
                yield self.stop_queue.put(to_stop)
                self.metrics['to_stop_commands'] += len(to_stop)

            if is_state_updated or to_run:
                yield self.adjust_queue.put(
                    (self.state, to_run, is_state_updated)
                )
                self.metrics['to_run_commands'] += len(to_run)


class AppsBaptizer(MetricsMixin):

    def __init__(self, logger, node, adjust_queue):
        super(AppsBaptizer, self).__init__()

        self.logger = logger

        self.node_service = node
        self.adjust_queue = adjust_queue

    @gen.coroutine
    def bless(self, app, profile=DEFAULT_RUN_PROFILE):
        try:
            yield self.node_service.start_app(app, profile)
            self.logger.info(SELF_NAME, 'starting app {} {}'.format(app, profile))
        except ServiceError as se:
            self.logger.warn(SELF_NAME, 'failed to start app {} {} with err: {}'
                .format(app, profile, e))

    @gen.coroutine
    def adjust(self, ch, app, to_adjust):
        try:
            print('bless: control {} {}'.format(app, to_adjust))
            yield ch.tx.write(to_adjust)
            self.logger.info(SELF_NAME, 'ajusting workers count {} {}'.format(app, to_adjust))
        except ServiceError as se:
            self.logger.error(SELF_NAME, 'failed to adjust app {} workers count for {} with err: {}'
                .format(app, to_adjust, se))

    @gen.coroutine
    def blessing_road(self):

        # TODO: method not implemented yet, using control_app as stub!
        control_channel = yield self.node_service.control('Echo')

        while True:
            new_state, to_run, do_adjust = yield self.adjust_queue.get()
            print(
                'bless: new_state {}, to_run {}, do_adjust {}'
                .format(new_state, to_run, do_adjust))

            try:
                yield [self.bless(app) for app in to_run]
                self.metrics['to_run_requests'] += len(to_run)

                if do_adjust:
                    self.metrics['state_update'] += 1
                    print('bless: got to adjust {}'.format(new_state))
                    yield [
                        self.adjust(control_channel, app, to_adjust)
                        for app, to_adjust in new_state.iteritems()]
            except Exception as e:
                print('bless: error while dispatching commands {}'.format(e))
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
                control_channel = yield self.node_service.control('Echo')
            finally:
                self.adjust_queue.task_done()


# Actually should be 'App Sleeper'
# TODO: look at tools for 'app stop' command sequence
class AppsSlayer(MetricsMixin):

    def __init__(self, logger, node, stop_queue):
        super(AppsSlayer, self).__init__()

        self.logger = logger

        self.node_service = node
        self.stop_queue = stop_queue

    @gen.coroutine
    def slay(self, app):
        print('slayer: stopping app {}'.format(app))
        self.metrics['slay_requests'] += 1
        yield self.node_service.pause_app(app)
        # ch = yield self.node_service.pause_app(app)
        # yield ch.rx.get()
        self.metrics['slayed'] += 1

    @gen.coroutine
    def topheth_road(self):
        while True:
            to_stop = yield self.stop_queue.get()

            try:
                yield [self.slay(app) for app in to_stop]
            except Exception as e:
                print('slayer: failed to stop one of the apps {}'.format(to_stop))
            finally:
                self.stop_queue.task_done()

@click.command()
@click.option(
    '--uuid',
    default=make_state_path(COCAINE_TEST_UUID), help='runtime uuid')
def main(uuid):
    input_queue = queues.Queue()
    adjust_queue = queues.Queue()
    stop_queue = queues.Queue()

    # TODO: names from config
    node = Service('node')
    # logging = Service('logging')
    logging = Logger()
    unicorn = Service('unicorn')

    acquirer = StateAcquirer(logging, node, unicorn, input_queue, uuid)
    state_processor = StateAggregator(logging, input_queue, adjust_queue, stop_queue)

    apps_slayer = AppsSlayer(logging, node, stop_queue)
    apps_baptizer = AppsBaptizer(logging, node, adjust_queue)

    # run async poll tasks in date flow reverse order, from sink to source
    IOLoop.current().spawn_callback(lambda: apps_slayer.topheth_road())
    IOLoop.current().spawn_callback(lambda: apps_baptizer.blessing_road())

    IOLoop.current().spawn_callback(lambda: state_processor.process_loop())

    IOLoop.current().spawn_callback(lambda: acquirer.poll_running_apps_list())
    IOLoop.current().spawn_callback(lambda: acquirer.subscribe_to_state_updates())

    qs = dict(input=input_queue, adjust=adjust_queue, stop=stop_queue)
    units = dict(
        acquisition=acquirer,
        state=state_processor,
        slayer=apps_slayer,
        baptizer=apps_baptizer)

    app = web.Application([
        (r'/state', StateHandler, ),
        (r'/metrics', MetricsHandler, dict(queues=qs, units=units))
    ])

    app.listen(DEFAULT_ORCA_PORT)
    IOLoop.current().start()

if __name__ == '__main__':
    main()
