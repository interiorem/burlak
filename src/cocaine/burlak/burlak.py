#
# TODO:
#   - timing metrics
#   - invalidate caches on runtime disconnection
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
from tornado import gen

from .chcache import ChannelsCache, close_tx_safe
from .logger import ConsoleLogger, VoidLogger
from .loop_sentry import LoopSentry

CONTROL_RETRY_ATTEMPTS = 3

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


def transmute_and_filter_state(input_state):
    '''Converts raw state dictionary to (app => StateRecords) mapping
    '''
    return {
        app: StateRecord(int(val['workers']), str(val['profile']))
        for app, val in input_state.iteritems()
        if val['workers'] >= 0
    }


class StateUpdateMessage(object):
    def __init__(self, state, version, uuid):
        self._state = transmute_and_filter_state(state)
        self._version = version
        self._uuid = uuid

    @property
    def state(self):
        return self._state

    @property
    def version(self):
        return self._version

    def get_all(self):
        return self._state, self._version, self._uuid


class MetricsMixin(object):
    def __init__(self, **kwargs):
        super(MetricsMixin, self).__init__(**kwargs)
        self.metrics_cnt = defaultdict(int)

    def get_count_metrics(self):
        return self.metrics_cnt


class LoggerMixin(object):  # pragma nocover
    def __init__(self, context, name=SELF_NAME, **kwargs):
        super(LoggerMixin, self).__init__(**kwargs)

        self.logger = context.logger_setup.logger
        self.format = '{} :: %s'.format(name)
        self.console = ConsoleLogger(context.config.console_log_level) \
            if context.logger_setup.dup_to_console \
            else VoidLogger()

    def debug(self, fmt, *args):
        self.console.debug(fmt, *args)
        self.logger.debug(self.format, fmt.format(*args))

    def info(self, fmt, *args):
        self.console.info(fmt, *args)
        self.logger.info(self.format, fmt.format(*args))

    def warn(self, fmt, *args):
        self.console.warn(fmt, *args)
        self.logger.warn(self.format, fmt.format(*args))

    def error(self, fmt, *args):
        self.console.error(fmt, *args)
        self.logger.error(self.format, fmt.format(*args))


class StateAcquirer(LoggerMixin, MetricsMixin, LoopSentry):

    TASK_NAME = 'state_subscriber'

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
            self, context, input_queue, **kwargs):
        super(StateAcquirer, self).__init__(context, **kwargs)

        self.input_queue = input_queue
        self.status = context.shared_status.register(StateAcquirer.TASK_NAME)

    @gen.coroutine
    def subscribe_to_state_updates(self, unicorn, uniresis, state_pfx):
        validator = Validator(StateAcquirer.STATE_SCHEMA)

        ch = None
        while self.should_run():
            try:
                self.status.mark_ok('getting uuid')

                self.debug('retrieving uuid from uniresis')
                uuid = yield uniresis.uuid()

                # TODO: validate uuid
                if not uuid:  # pragma nocover
                    self.error('got broken uuid')
                    self.status.mark_warn('got empty uuid')
                    yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
                    continue

                to_listen = make_state_path(state_pfx, uuid)

                self.status.mark_ok('subscribing for state')
                self.info('subscribing for path {}', to_listen)

                ch = yield unicorn.subscribe(to_listen)

                while self.should_run():
                    info_message = 'waiting for state updates'
                    self.status.mark_ok(info_message)
                    self.debug(info_message)

                    state, version = yield ch.rx.get()
                    self.debug(
                        'subscribe:: got version {} state {}', version, state)
                    self.status.mark_ok('processing state')

                    assert isinstance(version, int)

                    if not isinstance(state, dict):  # pragma nocover
                        self.error(
                            'expected dictionary, got {}',
                            type(state).__name__)
                        self.metrics_cnt['got_broken_sate'] += 1
                        raise Exception('state is empty, resubscribing')

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
                            'state not valid {} {}', state, validator.errors)
                        self.metrics_cnt['not_valid_state'] += 1
                        self.status.mark_warn('state not valid')

                    yield self.input_queue.put(
                        StateUpdateMessage(state, version, uuid))

                    self.metrics_cnt['apps_in_last_state'] = len(state)
            except Exception as e:  # pragma nocover
                self.error('failed to get state, error: "{}"', e)
                self.status.mark_warn('failed to get state')
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
            finally:  # pragma nocover
                # TODO: Is it really needed?
                yield close_tx_safe(ch)


class StateAggregator(LoggerMixin, MetricsMixin, LoopSentry):
    TASK_NAME = 'state_processor'

    def __init__(
            self,
            context,
            node,
            ci_state,
            input_queue, control_queue,
            poll_interval_sec,
            **kwargs):
        super(StateAggregator, self).__init__(context, **kwargs)

        self.context = context
        self.sentry_wrapper = context.sentry_wrapper

        self.node_service = node

        self.input_queue = input_queue
        self.control_queue = control_queue

        self.poll_interval_sec = poll_interval_sec

        self.ci_state = ci_state

        self.status = context.shared_status.register(StateAggregator.TASK_NAME)

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

        last_uuid = None
        while self.should_run():
            self.status.mark_ok('listening on incoming queue')

            is_state_updated = False
            msg = None
            uuid = None

            try:
                msg = yield self.input_queue.get(
                    timeout=timedelta(seconds=self.poll_interval_sec))
            except gen.TimeoutError:  # pragma nocover
                self.debug('input_queue timeout')
            else:
                self.input_queue.task_done()

            try:
                self.status.mark_ok('getting running apps list')

                self.ci_state.remove_old_stopped(
                    self.context.config.expire_stopped)

                running_apps = yield self.get_running_apps_set()

                self.debug('got running apps {}', running_apps)
                self.debug('last known uuid is {}', last_uuid)

                # Note that `StateUpdateMessage` only massage type currently
                # supported.
                if msg and isinstance(msg, StateUpdateMessage):
                    state, state_version, uuid = msg.get_all()
                    is_state_updated = True

                    self.debug(
                        'disp::got state update with version {}: {} uuid {} '
                        'and running apps {}',
                        state_version, state, uuid, running_apps)
            except Exception as e:
                self.error('failed to get control message with {}', e)
                self.sentry_wrapper.capture_exception()

            # Note that in general following code shouldn't raise.

            if not state:
                self.info(
                    'state not known yet, '
                    'skipping control iteration')
                continue

            self.status.mark_ok('processing state records')

            update_state_apps_set = set(state.iterkeys())

            to_run = update_state_apps_set - running_apps
            to_stop = running_apps - update_state_apps_set

            if last_uuid == uuid and prev_state == state:
                self.debug(
                    'got same state as in previous update iteration '
                    'for same uuid, skipping control step')
                is_state_updated = False

            if last_uuid != uuid:
                # uuid changed, it means we've reconnected,
                # reset committed state
                self.ci_state.reset()

            last_uuid = uuid

            if is_state_updated:  # check for profiles change
                to_update = self.make_prof_update_set(prev_state, state)

                to_run.update(to_update)
                to_stop.update(to_update)

                prev_state = state

                self.debug('profile update list {}', to_update)

            self.info("to_stop apps list {}", to_stop)
            self.info("to_run apps list {}", to_run)

            if is_state_updated or to_run or to_stop:
                self.status.mark_ok('sending processed state to dispatch')

                yield self.control_queue.put(
                    DispatchMessage(
                        state, state_version, is_state_updated,
                        to_stop, to_run
                    )
                )

                self.metrics_cnt['to_run_commands'] += len(to_run)
                self.metrics_cnt['to_stop_commands'] += len(to_stop)


class AppsElysium(LoggerMixin, MetricsMixin, LoopSentry):
    '''Controls life-time of applications based on supplied state
    '''
    TASK_NAME = 'tasks_dispatch'

    def __init__(
            self,
            context,
            ci_state,
            node,
            control_queue,
            **kwargs):
        super(AppsElysium, self).__init__(context, **kwargs)

        self.context = context
        self.sentry_wrapper = context.sentry_wrapper

        self.ci_state = ci_state

        self.node_service = node
        self.control_queue = control_queue

        self.status = context.shared_status.register(AppsElysium.TASK_NAME)

    @gen.coroutine
    def start(self, app, profile, state_version, tm, started=None):
        '''Trying to start application with specified profile
        '''
        try:
            ch = yield self.node_service.start_app(app, profile)
            yield ch.rx.get()
        except Exception as e:
            self.metrics_cnt['errors_start_app'] += 1
            self.error(
                'failed to start app {} {} with err: {}', app, profile, e)
            self.status.mark_warn('failed to start application')

            sentry_message = "can't start app {}, reason: {}".format(app, e)
            self.sentry_wrapper.capture_exception(message=sentry_message)

            self.ci_state.mark_failed(app, profile, state_version, tm)
        else:
            self.info('starting app {} with profile {}', app, profile)
            self.metrics_cnt['apps_started'] += 1

            if started is not None:
                started.add(app)

    @gen.coroutine
    def slay(self, app, state_version, tm):
        '''Stop/pause application
        '''
        try:
            ch = yield self.node_service.pause_app(app)
            yield ch.rx.get()

            self.ci_state.mark_stopped(app, state_version, tm)
            self.metrics_cnt['apps_stopped'] += 1

            self.info('app {} has been stopped', app)
        except Exception as e:  # pragma nocover
            self.error('failed to stop app {} with error: {}', app, e)
            self.metrics_cnt['errors_slay_app'] += 1

            self.sentry_wrapper.capture_exception()
            self.status.mark_warn('failed to stop application')

    @gen.coroutine
    def adjust_by_channel(
            self, app, channels_cache, to_adjust, profile, state_version, tm):

        self.debug('control command to {} with {}', app, to_adjust)

        attempts = CONTROL_RETRY_ATTEMPTS
        while attempts:
            try:
                ch = yield channels_cache.get_ch(app)
                yield ch.tx.write(to_adjust)
            except Exception as e:
                attempts -= 1
                self.error(
                    'failed to send control to `{}`, workers {}, '
                    'with attempts {}, err {}',
                    app, to_adjust, attempts, e)

                self.status.mark_crit('failed to send control command')
                self.metrics_cnt['errors_of_control'] += 1
                self.sentry_wrapper.capture_exception()

                yield channels_cache.close_and_remove([app])
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
            else:
                self.ci_state.mark_running(
                    app, to_adjust, profile, state_version, tm)

                self.debug(
                    'have adjusted workers count for app {} to {}',
                    app, to_adjust)

                break

    @gen.coroutine
    def blessing_road(self):
        channels_cache = ChannelsCache(self)

        while self.should_run():
            try:
                info_message = 'waiting for control command'
                self.debug(info_message)
                self.status.mark_ok(info_message)

                command = yield self.control_queue.get()
                self.control_queue.task_done()

                self.debug(
                    'control task: state {}, state_ver {}, do_adjust? {}, '
                    'to_stop {}, to_run {}',
                    command.state,
                    command.state_version,
                    command.is_state_updated,
                    command.to_stop,
                    command.to_run,
                )
                self.status.mark_ok('processing control command')

                self.ci_state.version = command.state_version

                yield channels_cache.close_and_remove(command.to_stop)

                if self.context.config.stop_apps:  # False by default
                    self.status.mark_ok('stopping apps')
                    tm = time.time()
                    yield [
                        self.slay(app, command.state_version, tm)
                        for app in command.to_stop
                    ]
                elif command.to_stop:
                    self.info(
                        'to_stop list not empty, '
                        'but stop_apps flag is disabled')

                # Should be an assertion if app is in to_run list, but not in
                # the state, sanity redundant check.
                self.status.mark_ok('starting apps')
                started = set()
                tm = time.time()
                yield [
                    self.start(
                        app,
                        command.state[app].profile, command.state_version,
                        tm,
                        started)
                    for app in command.to_run if app in command.state
                ]

                # Send control to every app in state, except known for
                # start up fail.
                to_control = set(command.state.iterkeys()) \
                    if command.is_state_updated else started
                failed_to_start = command.to_run - started

                if failed_to_start:  # pragma nocover
                    self.warn(
                        'control command will be skipped for '
                        'failed to start apps: {}',
                        failed_to_start)

                self.debug(
                    'control command will be send for apps: {}', to_control)

                if to_control:
                    self.status.mark_ok('adjusting workers count')
                    tm = time.time()
                    yield [
                        self.adjust_by_channel(
                            app, channels_cache,
                            int(state_record.workers), state_record.profile,
                            command.state_version, tm)
                        for app, state_record in command.state.iteritems()
                        if app in to_control and
                        app not in failed_to_start
                    ]

                    self.metrics_cnt['state_updates'] += 1
                    self.info('state updated')
            except Exception as e:  # pragma nocover
                self.error(
                    'failed to exec command with error {}: {}',
                    type(e).__name__, e)
                self.sentry_wrapper.capture_exception()
                self.status.mark_warn('failed to execute control command')

                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
