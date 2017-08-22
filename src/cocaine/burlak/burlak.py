#
# TODO:
#   - TBD
#   - use cerberus validator on inputed state
#   - console logger wrapper
#
# DONE:
#   - take start_app 'profile' from, emmm... state?
#   - get uuid from 'uniresis' (temporary proxy)
#   - expose state to web handle (partly implemented)
#   - use coxx logger
#   - secure service for 'unicorn'
#
import time

from collections import defaultdict, namedtuple

from cocaine.exceptions import CocaineError

from tornado import gen
from tornado.iostream import StreamClosedError

from .uniresis import catchup_an_uniresis


DEFAULT_RETRY_TIMEOUT_SEC = 10
DEFAULT_UNKNOWN_VERSIONS = 1

DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_EXP_BASE_SEC = 2


SELF_NAME = 'app/orca'  # aka 'Killer Whale'


def make_state_path(prefix, uuid):  # pragma nocover
    return prefix + '/' + uuid


DispatchMessage = namedtuple('DispatchMessage', [
    'state',
    'state_version',
    'is_state_updated',
    'to_stop',
    'to_run',
])


StateRecord = namedtuple('StateRecord', [
    'workers',
    'profile'
])


LoggerSetup = namedtuple('LoggerSetup', [
    'logger',
    'dup_to_console'
])


class RunningAppsMessage(object):
    def __init__(self, run_list=[]):
        self.running_apps = set(run_list)

    def get_apps_set(self):
        return self.running_apps


class StateUpdateMessage(object):
    def __init__(self, state, running_apps, version=-1):
        self.state = {
            app: StateRecord(workers, profile)
            for app, (workers, profile) in state.iteritems()
        }

        self.running_apps = running_apps
        self.version = version

    def get_state(self):
        return self.state

    def get_running_apps_set(self):
        return set(self.running_apps)

    def get_version(self):
        return self.version

    def get_all(self):
        return \
            self.get_state(), self.get_running_apps_set(), self.get_version()


class CommittedState(object):
    """
    State record format:
        <app name> : (<STATE>, <WORKERS COUNT>, <TIMESTAMP>)

        <STATE> - (RUNNING|STOPPED)
        <TIMESTAMP> - last state update time
    """

    NA_PROFILE_LABEL = 'n/a'

    def __init__(self):
        self.state = dict()

    def as_dict(self):
        return self.state

    def mark_running(self, app, workers, profile, state_version, tm):
        self.state.update(
            {app: ('RUNNING', workers, profile, state_version, int(tm))})

    def mark_stopped(self, app, state_version, tm):
        _, workers, profile, _, _ = self.state.get(
            app, ['', 0, self.NA_PROFILE_LABEL, 0, 0])

        self.state.update(
            {app: ('STOPPED', workers, profile, state_version, int(tm))})


class LoopSentry(object):  # pragma nocover
    def __init__(self, **kwargs):
        super(LoopSentry, self).__init__(**kwargs)
        self.run = True

    def should_run(self):
        return self.run

    def halt(self):
        self.run = False


class MetricsMixin(object):
    def __init__(self, **kwargs):
        super(MetricsMixin, self).__init__(**kwargs)
        self.metrics_cnt = defaultdict(int)

    def get_count_metrics(self):
        return self.metrics_cnt


class VoidLogger(object):  # pragma nocover
    def debug(self, msg):
        pass

    def info(self, msg):
        pass

    def warn(self, msg):
        pass

    def error(self, msg):
        pass


class ConsoleLogger(VoidLogger):  # pragma nocover
    def debug(self, msg):
        print('dbg: {}'.format(msg))

    def info(self, msg):
        print('info: {}'.format(msg))

    def warn(self, msg):
        print('warn: {}'.format(msg))

    def error(self, msg):
        print('error: {}'.format(msg))


class LoggerMixin(object):  # pragma nocover
    def __init__(self, logger_setup, name=SELF_NAME, **kwargs):
        super(LoggerMixin, self).__init__(**kwargs)

        self.logger = logger_setup.logger
        self.format = '{} :: %s'.format(name)
        self.console = ConsoleLogger() if logger_setup.dup_to_console \
            else VoidLogger()

    def debug(self, msg):
        self.console.debug(msg)
        self.logger.debug(self.format, msg)

    def info(self, msg):
        self.console.info(msg)
        self.logger.info(self.format, msg)

    def warn(self, msg):
        self.console.warn(msg)
        self.logger.warn(self.format, msg)

    def error(self, msg):
        self.console.error(msg)
        self.logger.error(self.format, msg)


class StateAcquirer(LoggerMixin, MetricsMixin, LoopSentry):
    def __init__(
            self,
            logger_setup,
            input_queue, poll_interval_sec,
            use_uniresis_stub=False,
            **kwargs):
        super(StateAcquirer, self).__init__(
            logger_setup, **kwargs)

        self.input_queue = input_queue

        self.poll_interval_sec = poll_interval_sec
        self.use_uniresis_stub = use_uniresis_stub

    @gen.coroutine
    def poll_running_apps_list(self, node_service):
        while self.should_run():
            try:
                self.debug('poll: getting apps list')

                ch = yield node_service.list()
                app_list = yield ch.rx.get()

                self.metrics_cnt['polled_running_nodes_count'] = len(app_list)
                self.info('getting apps list {}'.format(app_list))

                yield self.input_queue.put(RunningAppsMessage(app_list))
                yield gen.sleep(self.poll_interval_sec)
            except Exception as e:
                self.error('failed to poll apps list with "{}"'.format(e))
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)

    @gen.coroutine
    def subscribe_to_state_updates(self, unicorn, node, state_pfx):

        uniresis = catchup_an_uniresis(self.use_uniresis_stub)

        while self.should_run():
            try:
                uuid = yield uniresis.uuid()

                # TODO: validate uuid
                if not uuid:  # pragma nocover
                    self.error('got broken uuid')
                    yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
                    continue

                self.debug('subscription for path {}'.format(
                    make_state_path(state_pfx, uuid)))

                ch = yield unicorn.subscribe(make_state_path(state_pfx, uuid))

                while self.should_run():
                    state, version = yield ch.rx.get()

                    assert isinstance(version, int)
                    if not isinstance(state, dict):
                        self.error(
                            'expected dictionary, got {}'.format(
                                type(state).__name__))
                        self.metrics_cnt['got_broken_sate'] += 1
                        continue

                    self.debug(
                        'subscribe: got subscribed state {}'
                        .format(state))

                    node_ch = yield node.list()
                    app_list = yield node_ch.rx.get()

                    self.debug(
                        'got running apps on state update {}'
                        .format(app_list))

                    yield self.input_queue.put(
                        StateUpdateMessage(state, app_list, version))
                    self.metrics_cnt['last_state_app_count'] = len(state)

            except Exception as e:
                self.error(
                    'failed to get state subscription with "{}"'.format(e))
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)


class StateAggregator(LoggerMixin, MetricsMixin, LoopSentry):
    def __init__(
            self, logger_setup,
            input_queue, control_queue, **kwargs):
        super(StateAggregator, self).__init__(logger_setup, **kwargs)

        self.input_queue = input_queue
        self.control_queue = control_queue

    def make_prof_update_set(self, prev_state, state):
        to_update = []
        # Detect apps profile change
        for app, state_record in state.iteritems():
            prev_record = prev_state.get(app)
            if prev_record and prev_record.profile != state_record.profile:
                    # If profiles names for the same app is different
                    # upon updates, app must be stopped and restarted
                    # with new profile.
                    to_update.append(app)

        return set(to_update)

    @gen.coroutine
    def process_loop(self):

        running_apps = set()
        state, prev_state, state_version = (
            dict(), dict(), DEFAULT_UNKNOWN_VERSIONS
        )

        while self.should_run():
            is_state_updated = False
            msg = yield self.input_queue.get()

            try:
                if isinstance(msg, RunningAppsMessage):
                    running_apps = msg.get_apps_set()

                    assert isinstance(running_apps, set)
                    self.debug(
                        'disp::got running apps list {}'
                        .format(running_apps))
                elif isinstance(msg, StateUpdateMessage):
                    state, running_apps, state_version = msg.get_all()
                    is_state_updated = True

                    self.debug(
                        'disp::got state update with version {}: {} and '
                        'running apps {}'
                        .format(state_version, state, running_apps))
                else:
                    self.error('unknown message type {}'.format(msg))
            except Exception as e:
                self.error(
                    'failed to get control message with {}'
                    .format(e))
            finally:
                self.input_queue.task_done()

            if not state:
                self.info(
                    'state not known yet, '
                    'skipping control iteration')
                continue

            update_state_apps_set = set(state.iterkeys())

            to_run = update_state_apps_set - running_apps
            to_stop = running_apps - update_state_apps_set

            if is_state_updated:
                to_update = self.make_prof_update_set(prev_state, state)

                to_run.update(to_update)
                to_stop.update(to_update)

                prev_state = state

                self.debug('profile update list {}'.format(to_update))

            self.info("to_stop apps list {}".format(to_stop))
            self.info("to_run apps list {}".format(to_run))

            if is_state_updated or to_run or to_stop:
                yield self.control_queue.put(
                    DispatchMessage(
                        state, state_version, is_state_updated,
                        to_stop, to_run
                    )
                )
                self.metrics_cnt['total_run_app_commands'] += len(to_run)


class AppsElysium(LoggerMixin, MetricsMixin, LoopSentry):
    '''Controls life-time of applications based on supplied state
    '''
    def __init__(
            self,
            logger_setup,
            ci_state,
            node,
            control_queue,
            **kwargs):
        super(AppsElysium, self).__init__(logger_setup, **kwargs)

        self.ci_state = ci_state

        self.node_service = node
        self.control_queue = control_queue

    @gen.coroutine
    def bless(self, app, profile, tm):
        '''Trying to start application with specified profile
        '''
        try:
            yield self.node_service.start_app(app, profile)
            self.info(
                'starting app {} with profile {}'.format(app, profile))
            self.metrics_cnt['exec_run_app_commands'] += 1
        except CocaineError as ce:
            self.error(
                'failed to start app {} {} with err: {}'
                .format(app, profile, ce))

    @gen.coroutine
    def adjust(self, app, to_adjust, profile, state_version, tm):
        '''Control application workers count

        TODO: seen a pattern here, move attempts retry to external patch
        '''
        attempts = DEFAULT_RETRY_ATTEMPTS
        to_sleep = DEFAULT_RETRY_EXP_BASE_SEC

        while attempts:
            try:
                self.debug(
                    'control command to {} with {}'
                    .format(app, to_adjust))

                ch = yield self.node_service.control(app)
                yield ch.tx.write(to_adjust)

                self.ci_state.mark_running(
                    app, to_adjust, profile, state_version, tm)

                self.debug(
                    'have adjusted workers count for app {} to {}'
                    .format(app, to_adjust))
            except (StreamClosedError, CocaineError) as se:
                attempts -= 1

                self.error(
                    "failed to adjust app's {} workers "
                    "count to {} with err: {}, "
                    "attempts left {} "
                    .format(app, to_adjust, se, attempts))

                yield gen.sleep(to_sleep)
                to_sleep *= DEFAULT_RETRY_EXP_BASE_SEC

                assert attempts >= 0
            else:
                break

    @gen.coroutine
    def slay(self, app, state_version, tm):
        '''Stop/pause application
        '''
        try:
            yield self.node_service.pause_app(app)

            self.ci_state.mark_stopped(app, state_version, tm)
            self.metrics_cnt['apps_slayed'] += 1

            self.info('app {} has been stopped'.format(app))
        except Exception as e:
            self.error('failed to stop app {} with error: {}'.format(app, e))

    @gen.coroutine
    def filtered_control_apps(self, should_control_app, command):
        '''Control applications from filtered command.to_run list
        '''
        tm = time.time()
        yield [
            self.adjust(
                app, int(state_record.workers), state_record.profile,
                command.state_version, tm)
            for app, state_record in command.state.iteritems()
            if should_control_app(app)
        ]

    @gen.coroutine
    def blessing_road(self):

        # TODO: method not implemented yet, using control_app as stub!
        # control_channel = yield self.node_service.control('Echo')

        while self.should_run():
            command = yield self.control_queue.get()

            self.debug(
                'bless: state {}, state_ver {}, do_adjust? {}, '
                'to_stop {}, to_run {}'
                .format(
                    command.state,
                    command.state_version,
                    command.is_state_updated,
                    command.to_stop,
                    command.to_run,
                )
            )

            try:
                tm = time.time()
                yield [
                    self.slay(app, command.state_version, tm)
                    for app in command.to_stop
                ]

                # Should be an assertion if app is in to_run list, but not in
                # the state, sanity redundant check.
                tm = time.time()
                yield [
                    self.bless(app, command.state[app].profile, tm)
                    for app in command.to_run if app in command.state
                ]

                if command.is_state_updated:
                    # Send control to every app in state.
                    yield self.filtered_control_apps(
                        lambda _: True,
                        command)

                    self.metrics_cnt['state_updates_count'] += 1
                    self.info('state updated')
                elif command.to_run:
                    # If app was stopped (crushed), but state wasn't updated
                    # yet, just relaunch app and push last control to it.
                    yield self.filtered_control_apps(
                        lambda app: app in command.to_run,
                        command)

                    self.metrics_cnt['rerun_apps_count'] += len(command.to_run)
                    self.info('stopped apps rerunned')

            except Exception as e:
                self.error(
                    'failed to exec command with error {}: {}'
                    .format(type(e).__name__, e))

                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
                # TODO: can throw, do something?
                # control_channel = yield self.node_service.control('Echo')
            finally:
                self.control_queue.task_done()
