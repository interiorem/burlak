#
# TODO:
#   - use coxx logger
#   - take start_app 'profile' from, emmm... state?
#   - secure service for 'unicorn', maybe for 'node', 'logger'?
#   - expose state to andle
#
import time

from collections import defaultdict

from cocaine.exceptions import ServiceError

from tornado import gen
from tornado import web


DEFAULT_RETRY_TIMEOUT_SEC = 5
SELF_NAME = 'app/orca'  # aka Killer Whale


class MetricsHandler(web.RequestHandler):

    def initialize(self, queues, units):
        self.queues = queues
        self.units = units

    def get(self):
        metrics = {
            'queues_fill': {
                k: v.qsize() for k, v in self.queues.iteritems()
            },
            'metrics': {
                k: v.get_metrics() for k, v in self.units.iteritems()
            }
        }
        self.write(metrics)
        self.flush()


class StateHandler(web.RequestHandler):

    def initialize(self, committed_state):
        self.committed_state = committed_state

    def get(self):
        self.write(self.committed_state.as_dict())
        self.flush()


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


class CommittedState(object):
    """
        State record format:

            [<STATE>, <WORKERS COUNT>, <TIMESTAMP>]

        <STATE> - (RUNNING|STOPPED)
        <TIMESTAMP> - last state update time
    """
    def __init__(self):
        self.state = dict()

    def as_dict(self):
        return self.state

    def mark_running(self, app, workers, tm=time.time()):
        self.state.update({app: ['RUNNING', workers, int(tm)]})

    def mark_stopped(self, app, tm=time.time()):
        _, workers = self.state.get(app, ['', 0])
        self.state.update({app: ['STOPPED', workers, int(tm)]})


class CommonMixin(object):

    def __init__(self, logger, name=SELF_NAME):
        self.name = name
        self.format = name + ' :: %s'
        self.logger = logger

        self.metrics = defaultdict(int)

    def get_metrics(self):
        return self.metrics

    def debug(self, msg):
        print('dbg: {}'.format(msg))
        self.logger.debug(self.format, msg)

    def info(self, msg):
        print('info: {}'.format(msg))
        self.logger.info(self.format, msg)

    def warn(self, msg):
        print('warn: {}'.format(msg))
        self.logger.warn(self.format, msg)

    def error(self, msg):
        print('error: {}'.format(msg))
        self.logger.error(self.format, msg)


class StateAcquirer(CommonMixin):

    def __init__(self, logger, node, unicorn, input_queue, state_path,
                 poll_interval_sec):

        super(StateAcquirer, self).__init__(logger)

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

                self.metrics['polled_running_nodes_count'] = len(app_list)
                self.info('getting application list {}'.format(app_list))

                yield self.input_queue.put(RunningAppsMessage(app_list))
                yield gen.sleep(self.poll_interval_sec)
            except Exception as e:
                self.error('failed to poll apps list with {}'.format(e))
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)

    @gen.coroutine
    def subscribe_to_state_updates(self):
        ch = yield self.unicorn_service.subscribe(self.state_path)

        while True:
            try:
                result, _ = yield ch.rx.get()
                print('subscribe: got subscribed state {}'.format(result))
                yield self.input_queue.put(StateUpdateMessage(result))

                self.metrics['last_state_app_count'] = len(result)
            except Exception as e:
                print(
                    'subscribe: failed to get unicorn subscription with {}'
                    .format(e))

                self.error(
                    'failed to get state subscription with {}'.format(e))
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)

                # TODO: can throw?
                ch = yield self.unicorn_service.subscribe(self.state_path)


class StateAggregator(CommonMixin):

    def __init__(self, logger, input_queue, adjust_queue, stop_queue):
        super(StateAggregator, self).__init__(logger)

        self.logger = logger

        self.input_queue = input_queue
        self.stop_queue = stop_queue
        self.adjust_queue = adjust_queue

    @gen.coroutine
    def process_loop(self):

        running_apps = set()
        state = dict()

        while True:
            is_state_updated = False
            msg = yield self.input_queue.get()

            try:
                if isinstance(msg, RunningAppsMessage):
                    running_apps = msg.get_apps_set()
                    self.debug(
                        'disp::got running apps list {}'
                        .format(running_apps))
                elif isinstance(msg, StateUpdateMessage):
                    state = msg.get_state()
                    is_state_updated = True
                    self.debug(
                        'disp::got state update {}'.format(state))
                else:
                    self.error('unknown message type {}'.format(msg))
            except Exception as e:
                self.error(
                    'failed to get control message with {}'
                    .format(e))
            finally:
                self.input_queue.task_done()

            if not state:
                self.info("get running apps list, but don't have state yet")
                continue

            update_state_apps_set = set(state.iterkeys())

            to_run = update_state_apps_set - running_apps
            to_stop = running_apps - update_state_apps_set

            print('to_run {}'.format(to_run))
            print('to_stop {}'.format(to_stop))

            self.info("'to_stop' apps list {}".format(to_stop))
            self.info("'to_run' apps list {}".format(to_run))

            if to_stop:
                yield self.stop_queue.put(to_stop)
                self.metrics['total_stop_app_commands'] += len(to_stop)

            if is_state_updated or to_run:
                yield self.adjust_queue.put(
                    (state, to_run, is_state_updated))
                self.metrics['total_run_app_commands'] += len(to_run)


class AppsBaptizer(CommonMixin):

    def __init__(self, logger, ci_state, node, adjust_queue, default_profile):
        super(AppsBaptizer, self).__init__(logger)

        self.logger = logger
        self.ci_state = ci_state

        self.node_service = node
        self.adjust_queue = adjust_queue

        self.def_profile = default_profile

    @gen.coroutine
    def bless(self, app, profile, tm=time.time()):
        try:
            yield self.node_service.start_app(app, profile)
            self.info(
                'starting app {} with profile {}'.format(app, profile))
            self.metrics['exec_run_app_commands'] += 1
        except ServiceError as se:
            self.error(
                'failed to start app {} {} with err: {}'
                .format(app, profile, se))

    @gen.coroutine
    def adjust(self, app, to_adjust, tm=time.time()):
        try:
            print('bless: control to {} {}'.format(app, to_adjust))

            ch = yield self.node_service.control(app)
            yield ch.tx.write(to_adjust)

            self.ci_state.mark_running(app, to_adjust, tm)
            self.info(
                'adjusting workers count for app {} to {}'
                .format(app, to_adjust))
        except ServiceError as se:
            print("error while adjusting app {} {}".format(app, se))
            self.error(
                "failed to adjust app's {} workers count to {} with err: {}"
                .format(app, to_adjust, se))

    @gen.coroutine
    def blessing_road(self):

        # TODO: method not implemented yet, using control_app as stub!
        # control_channel = yield self.node_service.control('Echo')

        while True:
            new_state, to_run, do_adjust = yield self.adjust_queue.get()
            print(
                'bless: new_state {}, to_run {}, do_adjust {}'
                .format(new_state, to_run, do_adjust))

            try:
                tm = time.time()
                yield [self.bless(app, self.def_profile, tm) for app in to_run]

                if do_adjust:
                    yield [
                        self.adjust(app, int(to_adjust), tm)
                        for app, to_adjust in new_state.iteritems()]

                    self.metrics['state_updates_count'] += 1
                    self.info('state updated')
            except Exception as e:
                self.error('failed to exec command with error: {}'.format(e))
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
                # TODO: can throw, do something?
                # control_channel = yield self.node_service.control('Echo')
            finally:
                self.adjust_queue.task_done()


# Actually should be 'App Sleeper'
# TODO: look at tools for 'app stop' command sequence
class AppsSlayer(CommonMixin):

    def __init__(self, logger, ci_state, node, stop_queue):
        super(AppsSlayer, self).__init__(logger)

        self.logger = logger
        self.ci_state = ci_state

        self.node_service = node
        self.stop_queue = stop_queue

    @gen.coroutine
    def slay(self, app, tm=time.time()):
        print('slayer: stopping app {}'.format(app))
        try:
            yield self.node_service.pause_app(app)

            self.ci_state.mark_stopped(app, tm)
            self.metrics['apps_slayed'] += 1

            self.info('app {} has been stopped'.format(app))
        except ServiceError as se:
            self.error('failed to stop app {} with error: {}'.format(app, se))

    @gen.coroutine
    def topheth_road(self):
        while True:
            to_stop = yield self.stop_queue.get()

            try:
                tm = time.time()
                yield [self.slay(app, tm) for app in to_stop]
            except ServiceError as e:
                self.error(
                    'failed to stop one of the apps {}, error: {}'
                    .format(to_stop, e)
                )
            finally:
                self.stop_queue.task_done()
