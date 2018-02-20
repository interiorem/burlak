#
# TODO:
#
# DONE:
#   - invalidate caches on runtime disconnection
#   - timing metrics (seemingly working now)
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

import six

from tornado import gen

from .chcache import ChannelsCache, close_tx_safe
# Config imported for filter schema
from .config import Config
from .control_filter import ControlFilter
from .logger import ConsoleLogger, VoidLogger
from .loop_sentry import LoopSentry
from .patricia.patricia import trie


CONTROL_RETRY_ATTEMPTS = 3

DEFAULT_RETRY_TIMEOUT_SEC = 15
DEFAULT_UNKNOWN_VERSIONS = 1

DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_RETRY_EXP_BASE_SEC = 4

SYNC_COMPLETION_TIMEOUT_SEC = 600

SELF_NAME = 'app/orca'  # aka 'Killer Whale'


def make_state_path(prefix, uuid):  # pragma nocover
    return prefix + '/' + uuid


# TODO: Decompose!
DispatchMessage = namedtuple('DispatchMessage', [
    'state',
    'real_state',
    'state_version',
    'is_state_updated',
    'to_stop',
    'to_run',
    'runtime_reborn',
    'workers_mismatch',
    'stop_again',
    'broken_apps',
])


StateRecord = namedtuple('StateRecord', [
    'workers',
    'profile',
])


def build_trie(keys):
    t = trie()
    for k in keys:
        t[k] = None

    return t


def search_trie(t, prefixes):
    return [k for p in prefixes for k in t.iter(p)]


def find_keys_with_prefix(data, prefixes):
    t = build_trie(data)
    return search_trie(t, prefixes)


def filter_apps(apps, white_list):
    if not white_list:
        return apps

    def filter_dict(di, white_list):
        to_preserve = find_keys_with_prefix(di.viewkeys(), white_list)
        return {k: v for k, v in six.iteritems(di) if k in to_preserve}

    def filter_set(s, white_list):
        to_preserve = find_keys_with_prefix(s, white_list)
        return {item for item in s if item in to_preserve}

    if isinstance(apps, dict):
        return filter_dict(apps, white_list)
    elif isinstance(apps, set):
        return filter_set(apps, white_list)

    return apps


def update_fake_state(ci_state, version, real_state, control_state):
    to_fake_mark = real_state.viewkeys() - control_state.viewkeys()

    if to_fake_mark:
        ci_state.reset_output_state()
        ci_state.version = version

    now = time.time()
    # for app in control_state:
    #     ci_state.mark_stopped(app, version, now)

    for app in to_fake_mark:
        try:
            r = real_state[app]
            ci_state.mark_running(app, r.workers, r.profile, version, now)
        except KeyError:
            pass


#
# TODO: refactor someday as hugely inefficient and redundant conversion.
#
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


class ControlFilterMessage(object):
    def __init__(self, control_filter):
        self._control_filter = control_filter

    @property
    def control_filter(self):
        return self._control_filter


class ResetStateMessage(object):
    pass


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


class ControlFilterListener(LoggerMixin, MetricsMixin, LoopSentry):

    TASK_NAME = 'control_list_listener'

    FILTER_SCHEMA = Config.FILTER_SCHEMA

    def __init__(
            self, context, unicorn, filter_queue, input_queue, **kwargs):
        super(ControlFilterListener, self).__init__(context, **kwargs)

        self.context = context
        self.unicorn = unicorn

        # TODO: refactor as one single queue
        self.filter_queue = filter_queue
        self.input_queue = input_queue

        self.status = context.shared_status.register(
            ControlFilterListener.TASK_NAME)

    def validate_filter(self, validator, control_filter):
        if not isinstance(control_filter, dict):
            self.metrics_cnt['filter_wrong_type'] += 1
            raise Exception('control_filter is of wrong type')

        if not validator.validate({'control_filter': control_filter}):
            self.metrics_cnt['not_valid_control_filter'] += 1
            raise Exception(
                'control filter not valid {}, errors {}'
                .format(control_filter, validator.errors)
            )

    @gen.coroutine
    def send_filter(self, startup, control_filter):
        msg = ControlFilterMessage(control_filter)
        try:
            if startup:
                yield self.filter_queue.put(msg)
            else:
                yield self.input_queue.put(msg)
        except Exception as e:
            self.error('failed to send control filter, error {}', e)

        raise gen.Return(False)

    @gen.coroutine
    def subscribe_to_control_filter(self):
        ch = None
        startup = True
        validator = Validator(
            dict(control_filter=ControlFilterListener.FILTER_SCHEMA))

        was_an_error = False
        while self.should_run():
            try:
                path = self.context.config.control_filter_path

                info_message = 'subscribing for control filter'
                self.status.mark_ok(info_message)
                self.info('{} at {}', info_message, path)

                ch = yield self.unicorn.subscribe(path)

                while self.should_run():
                    info_message = 'waiting for control filter'
                    self.status.mark_ok(info_message)
                    self.debug(info_message)

                    control_filter, version = yield ch.rx.get()

                    # Throws on error
                    self.validate_filter(validator, control_filter)

                    control_filter = ControlFilter.from_dict(control_filter)
                    startup = yield self.send_filter(startup, control_filter)
                    self.metrics_cnt['control_filter_updates'] += 1
                    was_an_error = False

            except Exception as e:  # pragma nocover
                message = 'failed to get control filter from unicorn'
                self.status.mark_ok(message)
                self.info('{}, reason: "{}"', message, e)

                if not was_an_error:
                    startup = yield self.send_filter(
                        startup, self.context.config.control_filter)

                was_an_error = True
                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
            finally:
                yield close_tx_safe(ch)


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
                        self.metrics_cnt['got_broken_sate'] += 1
                        raise Exception(
                            'expected dictionary as state type, got {}'
                            .format(type(state).__name__)
                        )

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
                        self.metrics_cnt['not_valid_state'] += 1

                        error_message = 'state not valid'
                        self.status.mark_warn(error_message)
                        self.error(
                            '{}: {}, errors: {}',
                            error_message, state, validator.errors
                        )

                    yield self.input_queue.put(
                        StateUpdateMessage(state, version, uuid))

                    self.metrics_cnt['apps_in_last_state'] = len(state)
            except Exception as e:  # pragma nocover

                yield self.input_queue.put(ResetStateMessage())

                self.status.mark_warn('failed to get state')
                self.error('failed to get state, error: "{}"', e)

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
            filter_queue, input_queue, control_queue,
            poll_interval_sec,
            workers_distribution,
            **kwargs):
        super(StateAggregator, self).__init__(context, **kwargs)

        self.context = context
        self.sentry_wrapper = context.sentry_wrapper

        self.node_service = node

        self.filter_queue = filter_queue
        self.input_queue = input_queue
        self.control_queue = control_queue

        self.poll_interval_sec = poll_interval_sec
        self.ci_state = ci_state
        self.workers_distribution = workers_distribution

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

    def workers_diff(self, state, running_workers):
        '''Find mismatch between last state and current runtime state
        '''
        # In case if app is not in running_workers it would be started
        # on next control iteration, so it isn't any need to mark it
        # `failed` in common state.
        return {
            app
            for app, record in state.iteritems()
            if app in running_workers and
            abs(record.workers - running_workers[app])
        }

    @gen.coroutine
    def get_running_apps_set(self):
        ch = yield self.node_service.list()
        apps_list = yield ch.rx.get()

        raise gen.Return(set(apps_list))

    @gen.coroutine
    def get_info(self, app, flag=0x01):  # 0x01: collect overseer info
        ch = yield self.node_service.info(app, flag)
        info = yield ch.rx.get()
        raise gen.Return(info)

    @gen.coroutine
    def get_apps_info(self, apps):
        info = yield {a: self.get_info(a) for a in apps}
        raise gen.Return(info)

    def workers_per_app(self, info):

        def count_by_sum(record):  # pragma nocover
            pool = record.get('pool', dict())
            active = pool.get('active', 0)
            idle = pool.get('idle', 0)

            return active + idle

        def count_by_len(record):  # pragma nocover
            pool = record.get('pool', dict())
            slaves = pool.get('slaves', dict())
            return len(slaves)

        return {
            app: count_by_len(record)
            for app, record in info.iteritems()
        }

    def get_broken_apps(self, info):
        return {
            app
            for app, record in info.iteritems()
            if 'state' in record and record['state'] == 'broken'
        }

    @gen.coroutine
    def runtime_state(self, state, running_apps):
        info = yield self.get_apps_info(running_apps)
        workers_count = self.workers_per_app(info)

        broken_apps, stop_again = set(), set()

        if state:
            broken_apps = {
                app for app in self.get_broken_apps(info)
                if app in state
            }
            stop_again = {
                app for app in workers_count
                if app not in state and workers_count[app] > 0
            }

        workers_mismatch = self.workers_diff(state, workers_count)

        self.debug('in broken state {}', broken_apps)
        self.debug('current workers count {}', workers_count)
        self.debug('stop command should be resent to {}', stop_again)
        self.debug('workers mismatch {}', workers_mismatch)

        raise gen.Return((
            workers_count,
            workers_mismatch,
            stop_again,
            broken_apps,
        ))

    @gen.coroutine
    def get_filter_once(self):
        '''Get control filter on dispatch main loop startup
        '''
        # Get control filter on startup
        self.debug('waiting for init control_filter')

        msg = yield self.filter_queue.get()
        self.filter_queue.task_done()

        control_filter = msg.control_filter
        yield self.control_queue.put(msg)
        self.info('got init control_filter {}', msg.control_filter.as_dict())

        raise gen.Return(control_filter)

    @gen.coroutine
    def process_loop(self):
        running_apps = set()
        state, prev_state, real_state, state_version = (
            dict(), dict(), dict(), DEFAULT_UNKNOWN_VERSIONS
        )

        last_uuid = None
        control_filter = yield self.get_filter_once()

        while self.should_run():
            self.status.mark_ok('listenning on incoming queue')

            runtime_reborn = False
            is_state_updated = False

            # Note that uuid is used to determinate was it
            # any incoming state.
            uuid, msg = None, None
            workers_mismatch, stop_again, broken_apps = \
                [set() for _ in xrange(3)]

            try:
                msg = yield self.input_queue.get(
                    timeout=timedelta(seconds=self.poll_interval_sec))
            except gen.TimeoutError:  # pragma nocover
                self.debug('input_queue timeout')
            else:
                self.input_queue.task_done()

            try:
                self.status.mark_ok('getting running apps list')

                self.ci_state.remove_expired(
                    self.context.config.expire_stopped)

                # Note that `StateUpdateMessage` only massage type currently
                # supported.
                if isinstance(msg, StateUpdateMessage):
                    state, state_version, uuid = msg.get_all()
                    is_state_updated = True
                    self.ci_state.set_incoming_state(state, state_version)

                    control_state = \
                        filter_apps(state, control_filter.white_list)
                    real_state = state
                    state = control_state

                    self.debug(
                        'disp::got state update with version {} uuid {}: {}, '
                        'muted apps {}',
                        state_version, uuid, state,
                        real_state.viewkeys() - state.viewkeys()
                    )
                elif isinstance(msg, ResetStateMessage):
                    runtime_reborn = True

                    state.clear()
                    real_state.clear()

                    self.ci_state.reset()
                    self.workers_distribution.clear()
                    self.info('reset state signal')
                elif isinstance(msg, ControlFilterMessage):
                    control_filter = msg.control_filter
                    self.info(
                        'white_list updated signal {}',
                        control_filter.as_dict()
                    )
                    yield self.control_queue.put(msg)

                    # If it was some last state, apply it again with new
                    # control_filter settings.
                    if real_state:
                        self.info('resubmitting last state')
                        self.debug('previous state was {}', real_state)

                        state = \
                            filter_apps(real_state, control_filter.white_list)

                        uuid = last_uuid
                        is_state_updated = True

                running_apps = yield self.get_running_apps_set()
                pruned_running_apps = \
                    filter_apps(running_apps, control_filter.white_list)

                self.info(
                    'last uuid {}, running apps {}, muted apps {}',
                    last_uuid, running_apps,
                    running_apps - pruned_running_apps
                )

                running_apps = pruned_running_apps

                if not is_state_updated:
                    (
                        workers_count,
                        workers_mismatch,
                        stop_again,
                        broken_apps,
                    ) = yield self.runtime_state(state, running_apps)

                    self.workers_distribution.clear()
                    self.workers_distribution.update(workers_count)

            except Exception as e:
                self.error('failed to get control message with {}', e)
                self.sentry_wrapper.capture_exception()

            # Note that in general following code (up to the end of the
            # method) shouldn't raise.
            if not real_state:
                self.info('state not known yet, skipping control iteration')
                continue

            self.status.mark_ok('processing state records')

            update_state_apps_set = state.viewkeys()

            to_run = update_state_apps_set - running_apps
            to_stop = running_apps - update_state_apps_set

            if is_state_updated:  # check for profiles change
                to_update = self.make_prof_update_set(prev_state, state)

                to_run.update(to_update)
                to_stop.update(to_update)

                prev_state = state
                last_uuid = uuid

                self.debug('profile update list {}', to_update)

            self.info("to_stop apps list {}", to_stop)
            self.info("to_run apps list {}", to_run)

            if is_state_updated or to_run or to_stop:
                self.status.mark_ok('sending processed state to dispatch')

                # TODO: refact - make separate messages for each case.
                yield self.control_queue.put(
                    DispatchMessage(
                        state, real_state,
                        state_version, is_state_updated,
                        to_stop, to_run,
                        runtime_reborn,
                        workers_mismatch,
                        stop_again,
                        broken_apps,
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
            self.status.mark_warn('failed to start application')

            self.error(
                'failed to start app {} {} with err: {}', app, profile, e)

            self.sentry_wrapper.capture_exception(
                message="can't start app {}".format(app),
                extra=dict(
                    app=app,
                    profile=profile
                ),
            )

            self.ci_state.mark_failed(app, profile, state_version, tm, str(e))
        else:
            self.info('starting app {} with profile {}', app, profile)
            self.metrics_cnt['apps_started'] += 1

            if started is not None:
                started.add(app)

    @gen.coroutine
    def slay(self, app, state_version, tm, *unused):
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
    def stop_by_control(
            self, app, state_version, tm, channels_cache, stopped_by_control):
        '''Stop application with app.control(0)

        TODO:
            Apps will be periodically reported as running by RT,
            so would be scheduled to stop periodically (channels would be open,
            control would be send, channels would be closed).
        '''
        try:
            if app in stopped_by_control:
                return

            yield self.adjust_by_channel(
                app,
                '',
                channels_cache,
                0,
                state_version,
                tm,
            )

            self.ci_state.mark_stopped(app, state_version, tm)
            self.metrics_cnt['apps_stopped_by_control'] += 1
            stopped_by_control.add(app)

            self.info('app {} has been stopped with control', app)
        except Exception as e:  # pragma nocover
            self.error(
                'failed to stop app {} by control with error: {}', app, e)

            self.metrics_cnt['errors_stop_app_by_control'] += 1

            self.sentry_wrapper.capture_exception()
            self.status.mark_warn('failed to stop application by control')

    @gen.coroutine
    def control(self, ch, to_adjust):
        yield ch.tx.write(to_adjust)

    @gen.coroutine
    def control_with_ack(self, ch, to_adjust):  # pragma nocover
        '''Send control and get (dummy) result or exception

        TODO: tests

        '''
        yield ch.tx.write(to_adjust)
        yield ch.rx.get()

    @gen.coroutine
    def adjust_by_channel(
            self, app, profile, channels_cache, to_adjust, state_version, tm):

        self.debug(
            'control command to {} ack {} with {}',
            app, self.context.config.control_with_ack is True, to_adjust
        )

        control_method = self.control_with_ack \
            if self.context.config.control_with_ack else self.control

        attempts = CONTROL_RETRY_ATTEMPTS
        while attempts:
            try:
                ch = yield channels_cache.get_ch(app)
                yield control_method(ch, to_adjust)
            except Exception as e:
                attempts -= 1

                error_message = \
                    'send control has been failed for app `{}`, workers {}, ' \
                    'attempts left {}, error: {}' \
                    .format(app, to_adjust, attempts, e)

                self.error(error_message)

                self.status.mark_crit('failed to send control command')
                self.metrics_cnt['errors_of_control'] += 1
                self.sentry_wrapper.capture_exception()

                self.ci_state.mark_failed(
                    app, profile, state_version, tm, error_message)

                yield channels_cache.close_and_remove([app])
                yield gen.sleep(DEFAULT_RETRY_EXP_BASE_SEC)
            else:
                self.ci_state.mark_running(
                    app, to_adjust, profile, state_version, tm)

                self.debug(
                    'have adjusted workers count for app {} to {}',
                    app, to_adjust)

                break

    def mark_pending_stop(self, to_stop, state_version):
        now = time.time()
        for app in to_stop:
            self.ci_state.mark_pending_stop(app, state_version, now)

    def apply_filter(self, channels_cache, control_filter):
        self.info('got control filter {}'. control_filter.as_dict())
        if control_filter.white_list:
            yield channels_cache.close_other(control_filter.white_list)

    @gen.coroutine
    def blessing_road(self):
        channels_cache = ChannelsCache(self, self.node_service)
        stopped_by_control = set()

        control_filter = None

        while self.should_run():
            try:
                info_message = 'waiting for control command'
                self.debug(info_message)
                self.status.mark_ok(info_message)

                command = yield self.control_queue.get()
                self.control_queue.task_done()

                if isinstance(command, ControlFilterMessage):
                    # State is already pruned in dispatch in those case
                    control_filter = command.control_filter
                    self.apply_filter(channels_cache, control_filter)
                    continue
                elif not isinstance(command, DispatchMessage):
                    error_message = 'wrong command type in control subsystem'
                    self.error(
                        '{}: {}',
                        error_message, type(command).__name__)
                    self.status.mark_failed(error_message)
                    continue

                self.debug('control task: {}', command._asdict())
                self.status.mark_ok('processing control command')

                self.ci_state.version = command.state_version

                if command.is_state_updated:
                    update_fake_state(
                        self.ci_state,
                        command.state_version,
                        command.real_state,
                        command.state,
                    )

                    self.debug(
                        'updating fake state {} real state {}',
                        command.state,  # fake state
                        command.real_state,
                    )

                if command.runtime_reborn:
                    stopped_by_control.clear()

                stopped_by_control = stopped_by_control - command.stop_again

                if control_filter and not control_filter.apply_control:
                    self.info(
                        'got control command, but apply_control flag is off, '
                        'skipping control sequence')
                    update_fake_state(
                        self.ci_state,
                        command.state_version,
                        command.real_state,
                        dict(),
                    )

                    channels_cache.close_and_remove_all()
                    stop_by_control.clear()
                    continue

                #
                # Control commands follows
                #
                if self.context.config.stop_apps:  # False by default

                    self.status.mark_ok('stopping apps')
                    tm = time.time()

                    stop_method = self.stop_by_control \
                        if self.context.config.stop_by_control else self.slay

                    yield [
                        stop_method(
                            app,
                            command.state_version,
                            tm,
                            channels_cache,
                            stopped_by_control
                        )
                        for app in command.to_stop
                    ]

                elif command.to_stop:
                    self.info(
                        'to_stop list not empty, but stop_apps flag is off, '
                        'skipping `stop apps` stage')

                    # Default is `false`.
                    if self.context.config.pending_stop_in_state:
                        self.debug('mark prohibited to_stop apps in state')
                        self.mark_pending_stop(
                            command.to_stop, command.state_version)
                    else:
                        self.debug('remove prohibited to_stop apps from state')
                        self.ci_state.remove_listed(command.to_stop)

                if not self.context.config.stop_by_control:
                    # Close after `slay` method, but don't touch channels
                    # cache after `stop_by_control`.
                    self.debug('close and remove `to_stop` channels')
                    yield channels_cache.close_and_remove(command.to_stop)
                else:  # pragma nocover
                    # TODO: danger zone!
                    # Huge amount of channels could be leaked forever.
                    pass

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

                failed_to_start = command.to_run - started
                started = started | command.workers_mismatch

                # Send control to every app in state, except known for
                # start up fail.
                to_control = set(command.state.iterkeys()) \
                    if command.is_state_updated else started

                stopped_by_control = stopped_by_control - to_control
                self.debug('stopped_by_control {}', stopped_by_control)

                self.metrics_cnt['to_control'] = len(to_control)
                self.metrics_cnt['stopped_by_control'] = \
                    len(stopped_by_control)

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
                            app,
                            state_record.profile,
                            channels_cache,
                            int(state_record.workers),
                            command.state_version, tm)
                        for app, state_record in six.iteritems(command.state)
                        if app in to_control and
                        app not in failed_to_start
                    ]

                now = time.time()
                for app in command.broken_apps:
                    profile = command.state[app].profile \
                        if app in command.state else ''

                    self.ci_state.mark_failed(
                        app, profile, command.state_version, now,
                        "app is in broken state")

                self.metrics_cnt['state_updates'] += 1
                self.metrics_cnt['ch_cache_size'] += len(channels_cache)

                self.info('state updated')
            except Exception as e:  # pragma nocover
                self.error(
                    'failed to exec command with error {}: {}',
                    type(e).__name__, e)
                self.sentry_wrapper.capture_exception()
                self.status.mark_warn('failed to execute control command')

                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
