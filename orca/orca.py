#
# TODO:
#   - use coxx logger
#   - take start_app 'profile' from, emmm... state?
#   - secure service for 'unicorn', maybe for 'node', 'logger'?
#
from tornado import gen
from tornado.ioloop import IOLoop
from tornado import web
from tornado import queues

from cocaine.services import Service
from cocaine.exceptions import ServiceError
from cocaine.exceptions import DisconnectionError


APP_LIST_POLL_INTERVAL = 5
DEFAULT_RETRY_TIMEOUT_SEC = 5

UNICORN_STATE_PREFIX = '/state/prefix'
COCAINE_TEST_UUID = 'SOME_UUID'
DEFAULT_RUN_PROFILE = 'DefaultProfile'
DEFAULT_ORCA_PORT = 8877


def make_state_path(uuid):
    return UNICORN_STATE_PREFIX + '/' + uuid

class MetricsHandler(web.RequestHandler):

    def initialize(self, queues):
        self.queues = queues

    def get(self):
        metrics = dict(
            queues_fill = {
                k : v.qsize() for k, v in self.queues.iteritems()
            },
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


class StateAcquirer(object):

    def __init__(self, logger, node, unicorn, input_queue, state_path, poll_interval_sec=APP_LIST_POLL_INTERVAL):
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
                result, _ = yield ch.rx.get()
                print('subscribe: got subscribed state {}'.format(result))
                yield self.input_queue.put(StateUpdateMessage(result))
            except Exception as e:
                print('subscribe: failed to get unicorn subscription with {}'.format(e))
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
                # TODO: remove previous subscription?
                ch = yield self.unicorn_service.subscribe(self.state_path)


class StateAggregator(object):

    def __init__(self, logger, input_queue, adjust_queue, stop_queue):
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
                    print('disp: got running apps list {}'.format(self.running_apps_set))
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

            if is_state_updated or to_run:
                yield self.adjust_queue.put((self.state, to_run, is_state_updated))


class AppsBlesser(object):

    def __init__(self, logger, node, adjust_queue):
        self.logger = logger

        self.node_service = node
        self.adjust_queue = adjust_queue

    @gen.coroutine
    def blessing_road(self):

        # TODO: method not implemented yet, using control_app as stub!
        control_channel = yield self.node_service.control('Echo')

        while True:

            print('bless: before state')
            new_state, to_run, do_adjust = yield self.adjust_queue.get()
            print('bless: new_state {}, to_run {}, do_adjust {}'.format(new_state, to_run, do_adjust))

            try:
                if to_run:
                    print('bless: got to run {}'.format(to_run))
                    for app in to_run:
                        print('bless: running app {}'.format(app))
                        yield self.node_service.start_app(app, DEFAULT_RUN_PROFILE)

                if do_adjust:
                    print('bless: got to adjust {}'.format(new_state))
                    for app, to_adjust in new_state.iteritems():
                        print('bless: setting for app {} workers num {}'.format(app, to_adjust))
                        # TODO: check 'to_adjust' value
                        # TODO: must be .write(app, to_adjust)
                        yield control_channel.tx.write(to_adjust)

            except Exception as e:
                print('bless: error while processing commands {}'.format(e))
                control_channel = yield self.node_service.control('Echo')
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
            finally:
                self.adjust_queue.task_done()


# Actually should be 'App Sleeper'
# TODO: look at tools for 'app stop' command sequence
class AppsSlayer(object):

    def __init__(self, logger, node, stop_queue):
        self.logger = logger

        self.node_service = node
        self.stop_queue = stop_queue

    @gen.coroutine
    def topheth_road(self):
        while True:
            to_stop = yield self.stop_queue.get()

            try:
                for app in to_stop:
                    yield self.node_service.pause_app(app)
            except Exception as e:
                print('failed to stop one of the apps {}'.format(to_stop))
            finally:
                self.stop_queue.task_done()

def main():
    input_queue = queues.Queue()
    adjust_queue = queues.Queue()
    stop_queue = queues.Queue()

    # TODO: names from config
    node = Service('node')
    logging = Service('logging')
    unicorn = Service('unicorn')

    acquirer = StateAcquirer(logging, node, unicorn, input_queue, make_state_path(COCAINE_TEST_UUID))
    state_processor = StateAggregator(logging, input_queue, adjust_queue, stop_queue)

    apps_slayer = AppsSlayer(logging, node, stop_queue)
    apps_baptizer = AppsBlesser(logging, node, adjust_queue)

    # run async poll tasks in date flow reverse order, from sink to source
    IOLoop.current().spawn_callback(lambda: apps_slayer.topheth_road())
    IOLoop.current().spawn_callback(lambda: apps_baptizer.blessing_road())

    IOLoop.current().spawn_callback(lambda: state_processor.process_loop())

    IOLoop.current().spawn_callback(lambda: acquirer.poll_running_apps_list())
    IOLoop.current().spawn_callback(lambda: acquirer.subscribe_to_state_updates())

    qs = dict(input=input_queue, adjust=adjust_queue, stop=stop_queue)

    app = web.Application([
        (r'/state', StateHandler, ),
        (r'/metrics', MetricsHandler, dict(queues=qs))
    ])

    app.listen(DEFAULT_ORCA_PORT)
    IOLoop.current().start()

if __name__ == '__main__':
    main()
