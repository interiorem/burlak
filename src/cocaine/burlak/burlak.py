#
# TODO:
#   - timing metrics
#
# DONE:
#   - console logger wrapper
#   - use cerberus validator on inputed state
#   - take start_app 'profile' from, emmm... state?
#   - get uuid from 'uniresis' (temporary proxy)
#   - expose state to web handle (partly implemented)
#   - use coxx logger
#   - secure service for 'unicorn'
#
import time

from collections import defaultdict, namedtuple
from datetime import timedelta

from cerberus import Validator
from cocaine.exceptions import CocaineError

from tornado import gen

from .chcache import ChannelsCache, close_tx_safe


CONTROL_RETRY_ATTEMPTS = 10

DEFAULT_RETRY_TIMEOUT_SEC = 10
DEFAULT_UNKNOWN_VERSIONS = 1

DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_RETRY_EXP_BASE_SEC = 2

SYNC_COMPLETION_TIMEOUT_SEC = 600

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
    'profile',
])


LoggerSetup = namedtuple('LoggerSetup', [
    'logger',
    'dup_to_console',
])


def transmute_and_filter_state(input_state):
    '''Converts raw state dictionary to (app => StateRecords) mapping
    '''
    return {
        app: StateRecord(int(val['workers']), str(val['profile']))
        for app, val in input_state.iteritems()
        if val['workers'] >= 0
    }


class StateUpdateMessage(object):
    def __init__(self, state, version=-1):
        self.state = transmute_and_filter_state(state)
        self.version = version

    def get_state(self):
        return self.state

    def get_version(self):
        return self.version

    def get_all(self):
        return self.get_state(), self.get_version()


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
        print('{} dbg: {}'.format(int(time.time()), msg))

    def info(self, msg):
        print('{} info: {}'.format(int(time.time()), msg))

    def warn(self, msg):
        print('{} warn: {}'.format(int(time.time()), msg))

    def error(self, msg):
        print('{} error: {}'.format(int(time.time()), msg))


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

    STATE_SCHEMA = {
        'state': {
            'type': 'dict',
            'valueschema': {
                'type': 'dict',
                'schema': {
                    'profile': {
                        'type': 'string',
                    },
                    'workers': {
                        'type': 'integer',
                        'min': 0,
                    },
                },
            },
        },
    }

    def __init__(
            self, logger_setup, input_queue, **kwargs):
        super(StateAcquirer, self).__init__(logger_setup, **kwargs)
        self.input_queue = input_queue

    @gen.coroutine
    def subscribe_to_state_updates(self, unicorn, node, uniresis, state_pfx):
        validator = Validator(StateAcquirer.STATE_SCHEMA)

        ch = None
        while self.should_run():
            try:
                self.debug('retrieving uuid from uniresis')
                uuid = yield uniresis.uuid()

                # TODO: validate uuid
                if not uuid:  # pragma nocover
                    self.error('got broken uuid')
                    yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
                    continue

                to_listen = make_state_path(state_pfx, uuid)
                self.info('subscribing for path {}'.format(to_listen))
                ch = yield unicorn.subscribe(to_listen)

                while self.should_run():
                    self.debug('waiting for state subscription')
                    state, version = yield ch.rx.get()
                    self.debug(
                        'subscribe:: got version {} state {}'
                        .format(version, state))

                    assert isinstance(version, int)

                    if not isinstance(state, dict):  # pragma nocover
                        self.error(
                            'expected dictionary, got {}'.format(
                                type(state).__name__))
                        self.metrics_cnt['got_broken_sate'] += 1
                        raise Exception('state is empty, resubscribe')

                    #
                    # Bench results:
                    # dict with 1000 records (apps) is validated for ~ 100 ms
                    # on core-i7 notebook.
                    #
                    if not validator.validate({'state': state}):
                        # If state isn't valid, report to log as error, but
                        # try to continue as it possible that
                        # 'transmute_and_filter_state' will correct/coerse
                        # state records to normal format, if not, it would be
                        # exception in StateUpdateMessage ctor.
                        self.error(
                            'state not valid {} {}'
                            .format(state, validator.errors))
                        self.metrics_cnt['not_valid_state'] += 1

                    yield self.input_queue.put(
                        StateUpdateMessage(state, version))

                    self.metrics_cnt['last_state_app_count'] = len(state)
            except Exception as e:  # pragma nocover
                self.error('failed to get state, error: "{}"'.format(e))
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
            finally:  # pragma nocover
                # TODO: Is it really needed?
                yield close_tx_safe(ch)


class StateAggregator(LoggerMixin, MetricsMixin, LoopSentry):
    def __init__(
            self,
            node,
            logger_setup,
            input_queue, control_queue, sync_queue,
            poll_interval_sec,
            **kwargs):
        super(StateAggregator, self).__init__(logger_setup, **kwargs)

        self.node_service = node

        self.input_queue = input_queue
        self.control_queue = control_queue
        self.sync_queue = sync_queue

        self.poll_interval_sec = poll_interval_sec

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
    def get_running_apps_set(self):
        ch = yield self.node_service.list()
        apps_list = yield ch.rx.get()

        raise gen.Return(set(apps_list))

    @gen.coroutine
    def process_loop(self):

        running_apps = set()
        state, prev_state, state_version = (
            dict(), dict(), DEFAULT_UNKNOWN_VERSIONS
        )

        while self.should_run():
            # used for signaling `queue get` event to run `task_done` later.
            is_state_updated = False
            msg = None

            try:
                msg = yield self.input_queue.get(
                    timeout=timedelta(seconds=self.poll_interval_sec))
            except gen.TimeoutError:
                self.info('input_queue timeout')
            else:
                self.input_queue.task_done()

            try:
                running_apps = yield self.get_running_apps_set()
                self.debug('got running apps list {}'.format(running_apps))

                # Note that `StateUpdateMessage` only massage type currently
                # supported.
                if msg and isinstance(msg, StateUpdateMessage):
                    state, state_version = msg.get_all()
                    is_state_updated = True

                    self.debug(
                        'disp::got state update with version {}: {} and '
                        'running apps {}'
                        .format(state_version, state, running_apps))
            except Exception as e:
                self.error(
                    'failed to get control message with {}'
                    .format(e))

            if not state:
                self.info(
                    'state not known yet, '
                    'skipping control iteration')
                continue

            update_state_apps_set = set(state.iterkeys())

            to_run = update_state_apps_set - running_apps
            to_stop = running_apps - update_state_apps_set

            if is_state_updated:  # check for porfiles change
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

                try:
                    # Wait for command completion to avoid races in
                    # node service.
                    self.debug('waiting for command execution completion...')
                    yield self.sync_queue.get(
                        timeout=timedelta(seconds=SYNC_COMPLETION_TIMEOUT_SEC))
                    self.sync_queue.task_done()
                    self.debug('command completed')
                except gen.TimeoutError:
                    self.error(
                        'fatal error: '
                        'command execution completion timeout')


class AppsElysium(LoggerMixin, MetricsMixin, LoopSentry):
    '''Controls life-time of applications based on supplied state
    '''
    def __init__(
            self,
            logger_setup,
            ci_state,
            node, node_ctl,
            control_queue, sync_queue,
            **kwargs):
        super(AppsElysium, self).__init__(logger_setup, **kwargs)

        self.ci_state = ci_state

        self.node_service = node
        self.node_service_ctl = node
        self.control_queue = control_queue
        self.sync_queue = sync_queue

    @gen.coroutine
    def start(self, app, profile, tm):
        '''Trying to start application with specified profile
        '''
        try:
            ch = yield self.node_service.start_app(app, profile)
            yield ch.rx.get()

            self.info(
                'starting app {} with profile {}'.format(app, profile))
            self.metrics_cnt['exec_run_app_commands'] += 1
        except CocaineError as ce:
            self.error(
                'failed to start app {} {} with err: {}'
                .format(app, profile, ce))

    @gen.coroutine
    def slay(self, app, state_version, tm):
        '''Stop/pause application
        '''
        try:
            ch = yield self.node_service.pause_app(app)
            yield ch.rx.get()

            self.ci_state.mark_stopped(app, state_version, tm)
            self.metrics_cnt['apps_slayed'] += 1

            self.info('app {} has been stopped'.format(app))
        except Exception as e:  # pragma nocover
            self.error('failed to stop app {} with error: {}'.format(app, e))

    @gen.coroutine
    def adjust_by_channel(
            self, app, channels_cache, to_adjust, profile, state_version, tm):

        self.debug('control command to {} with {}'.format(app, to_adjust))

        attempts = CONTROL_RETRY_ATTEMPTS
        while attempts:
            try:
                ch = yield channels_cache.get_ch(app)
                yield ch.tx.write(to_adjust)
            except Exception as e:
                attempts -= 1
                self.error(
                    'failed to send control to `{}` with attempts {}, err {}'
                    .format(app, attempts, e))

                yield channels_cache.close_one(app)
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
            else:
                self.ci_state.mark_running(
                    app, to_adjust, profile, state_version, tm)

                self.debug(
                    'have adjusted workers count for app {} to {}'
                    .format(app, to_adjust))

                break

    @gen.coroutine
    def blessing_road(self):

        # TODO: method not implemented yet, using control_app as stub!
        # control_channel = yield self.node_service.control('Echo')

        channels_cache = ChannelsCache(self, self.node_service_ctl)

        while self.should_run():
            try:
                self.debug('waiting for control command...')
                command = yield self.control_queue.get()

                self.debug(
                    'control task: state {}, state_ver {}, do_adjust? {}, '
                    'to_stop {}, to_run {}'
                    .format(
                        command.state,
                        command.state_version,
                        command.is_state_updated,
                        command.to_stop,
                        command.to_run,
                    )
                )

                yield channels_cache.close_and_remove(command.to_stop)

                tm = time.time()
                yield [
                    self.slay(app, command.state_version, tm)
                    for app in command.to_stop
                ]

                # Should be an assertion if app is in to_run list, but not in
                # the state, sanity redundant check.
                tm = time.time()
                yield [
                    self.start(app, command.state[app].profile, tm)
                    for app in command.to_run if app in command.state
                ]

                if command.is_state_updated or command.to_run:
                    # Send control to every app in state.
                    tm = time.time()
                    yield [
                        self.adjust_by_channel(
                            app, channels_cache,
                            int(state_record.workers), state_record.profile,
                            command.state_version, tm)
                        for app, state_record in command.state.iteritems()
                    ]

                    self.metrics_cnt['state_updates_count'] += 1
                    self.info('state updated')
            except Exception as e:  # pragma nocover
                self.error(
                    'failed to exec command with error {}: {}'
                    .format(type(e).__name__, e))

                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
            finally:
                self.debug('sending sync...')
                self.control_queue.task_done()
                yield self.sync_queue.put(True)
                self.debug('send sync ack')
