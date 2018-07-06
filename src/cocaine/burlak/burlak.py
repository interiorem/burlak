#
# TODO:
#   - file too long! refactor to different project files
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

from cocaine.exceptions import ServiceError
from collections import defaultdict, namedtuple
from datetime import timedelta

from cerberus import Validator

import six

from tornado import gen
from tornado import locks

from .chcache import ChannelsCache, close_tx_safe
# Config imported for filter schema
from .config import Config
from .control_filter import ControlFilter
from .dumper import Dumper
from .logger import ConsoleLogger, VoidLogger
from .loop_sentry import LoopSentry
from .patricia.patricia import trie
from .mixins import *


CONTROL_RETRY_ATTEMPTS = 3

DEFAULT_RETRY_TIMEOUT_SEC = 15
DEFAULT_UNKNOWN_VERSIONS = 1

DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_RETRY_EXP_BASE_SEC = 4

SYNC_COMPLETION_TIMEOUT_SEC = 600

INVALID_STATE_ERR_CODE = 6


# TODO: Decompose!
DispatchMessage = namedtuple('DispatchMessage', [
    'state',
    'real_state',
    'state_version',
    'is_state_updated',
    'to_hard_stop',
    'to_stop',
    'to_run',
    'runtime_reborn',
    'workers_mismatch',
    'stop_again',
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
    return [k for p in prefixes for k in t.iter(p) if p]


def find_keys_with_prefix(data, prefixes):
    t = build_trie(data)
    return search_trie(t, prefixes)


def filter_apps(apps, white_list):
    if not white_list:
        return apps

    def filter_dict(di, white_list):
        to_preserve = find_keys_with_prefix(di.iterkeys(), white_list)
        return {k: v for k, v in six.iteritems(di) if k in to_preserve}

    def filter_set(s, white_list):
        to_preserve = find_keys_with_prefix(s, white_list)
        return {item for item in s if item in to_preserve}

    if isinstance(apps, dict):
        return filter_dict(apps, white_list)
    elif isinstance(apps, set):
        return filter_set(apps, white_list)
    elif isinstance(apps, list):
        return list(filter_set(set(apps), white_list))

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
        # to_fake_mark is always subset of real_state, so no check for
        # KeyError exception is needed.
        r = real_state[app]
        ci_state.mark_running(app, r.workers, r.profile, version, now)


#
# TODO: refactor someday as hugely inefficient and redundant conversion.
#
def transmute_and_filter_state(input_state):
    """Converts raw state dictionary to (app => StateRecords) mapping."""
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


class NoStateNodeMessage(object):
    pass


class DumpCommittedState(object):
    pass


class ControlFilterListener(LoggerMixin, MetricsMixin, LoopSentry):

    TASK_NAME = 'control_list_listener'

    FILTER_SCHEMA = Config.FILTER_SCHEMA

    VersionedFilter = namedtuple('VersionedFilter', [
        'control_filter',
        'version'
    ])

    def __init__(
            self, context,
            unicorn,
            filter_queue, input_queue,
            **kwargs):
        super(ControlFilterListener, self).__init__(context, **kwargs)

        self.context = context
        self.unicorn = unicorn

        # TODO: refactor as one single queue
        self.filter_queue = filter_queue
        self.input_queue = input_queue

        self.status = \
            context.shared_status.register(ControlFilterListener.TASK_NAME)

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

        self.debug(
            'sending control_filter start_flag {}, {} ',
            startup, control_filter
        )

        msg = ControlFilterMessage(control_filter)

        try:
            if startup:
                yield self.filter_queue.put(msg)
            else:
                yield self.input_queue.put(msg)
        except Exception as e:  # pragma nocover
            self.error('failed to send control filter, error {}', e)

        raise gen.Return(False)

    @gen.coroutine
    def subscribe_to_control_filter(self):
        ch = None
        startup = True
        validator = Validator(
            dict(control_filter=ControlFilterListener.FILTER_SCHEMA))

        was_an_error = False
        last_filter = None

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

                    control_filter, version = \
                        yield ch.rx.get(
                            timeout=self.context.config.api_timeout_by2)

                    current_filter = ControlFilterListener.VersionedFilter(
                        control_filter,
                        version
                    )

                    if current_filter == last_filter:
                        self.info(
                            'control_filter {} with version {} '
                            'already processed, ignoring',
                            current_filter.control_filter,
                            version)
                        continue

                    last_filter = current_filter

                    # Throws on error
                    self.validate_filter(validator, control_filter)
                    control_filter = ControlFilter.from_dict(control_filter)
                    startup = yield self.send_filter(startup, control_filter)
                    self.metrics_cnt['control_filter_updates'] += 1
                    was_an_error = False
            except gen.TimeoutError as e:
                self.debug('control_filter subscription expired {}', e)
                self.metrics_cnt['control_filter_timeout_error'] += 1
            except Exception as e:

                message = 'using default control filter'

                self.status.mark_ok(message)
                self.info('{}, reason: "{}"', message, e)

                if not was_an_error:
                    startup = yield self.send_filter(
                        startup, self.context.config.control_filter)

                was_an_error = True
                last_filter = None

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

    VersionedState = namedtuple('VersionedState', [
        'uuid',
        'state',
        'version',
    ])

    def __init__(
            self, context, sharding_setup, input_queue, **kwargs):
        super(StateAcquirer, self).__init__(context, **kwargs)

        self.context = context
        self.input_queue = input_queue
        self.sharding_setup = sharding_setup

        self.status = context.shared_status.register(StateAcquirer.TASK_NAME)

    @gen.coroutine
    def subscribe_to_state_updates(self, unicorn):
        validator = Validator(StateAcquirer.STATE_SCHEMA)

        ch = None
        last_state = None

        while self.should_run():
            try:
                self.status.mark_ok('getting `state` path')

                uuid, to_listen = yield self.sharding_setup.get_state_route()
                if not (uuid and to_listen):
                    self.error(
                        'got broken state route, uuid {} path {}',
                        uuid, to_listen
                    )
                    self.status.mark_warn('got broken state listen route')
                    yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
                    continue

                if last_state and last_state.uuid != uuid:
                    # It was some uuid already, but new one has came,
                    # reset feedback state.
                    last_state = None
                    self.debug('runtime uuid has been changed')
                    yield self.input_queue.put(ResetStateMessage())

                if last_state is None or last_state.uuid != uuid:
                    # new uuid, recreate feedback node in unicorn
                    self.debug('signal to dump feedback')
                    yield self.input_queue.put(DumpCommittedState())

                self.status.mark_ok('subscribing for state')
                self.info('subscribing for path {}', to_listen)

                ch = yield unicorn.subscribe(to_listen)

                while self.should_run():
                    info_message = 'waiting for state updates'
                    self.status.mark_ok(info_message)
                    self.debug(info_message)

                    state, version = yield ch.rx.get(
                            timeout=self.context.config.api_timeout_by2)

                    assert isinstance(version, int)

                    if state is None and version == -1:
                        self.metrics_cnt['empty_state_node'] += 1
                        self.info('state was possibly removed')

                        if last_state and last_state.uuid == uuid:
                            # The same uuid, but state node has gone,
                            # reset feedback and send control(0) to all
                            # possessed apps.
                            last_state = None
                            self.debug('state record was actually removed')
                            yield self.input_queue.put(NoStateNodeMessage())

                        yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)

                        continue

                    if not isinstance(state, dict):  # pragma nocover
                        self.metrics_cnt['non_valid_state'] += 1
                        raise Exception(
                            'expected dictionary as state type, got {}'
                            .format(type(state).__name__)
                        )

                    current_state = \
                        StateAcquirer.VersionedState(uuid, state, version)

                    if current_state == last_state:
                        self.info(
                            'state version {} already processed, ignoring',
                            version
                        )
                        continue

                    last_state = current_state

                    self.debug(
                        'subscribe:: got version {} state {}', version, state)
                    self.status.mark_ok('processing state')

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

                    # StateUpdateMessage should throw in ctor on wrong state
                    # format
                    yield self.input_queue.put(
                        StateUpdateMessage(state, version, uuid))

                    self.metrics_cnt['apps_in_last_state'] = len(state)
            except gen.TimeoutError as e:
                self.metrics_cnt['state_timeout_error'] += 1
                self.debug('state subscription expired {}', e)
            except Exception as e:  # pragma nocover
                self.status.mark_warn('state not ready')
                self.error('failed to get state, exception: "{}"', e)

                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
            finally:  # pragma nocover
                # TODO: Is it really needed?
                yield close_tx_safe(ch)


class MetricsFetcher(LoggerMixin, MetricsMixin, LoopSentry):
    '''
    TODO: WIP, not yet implemented, nor tested
    '''
    def __init__(self, context, source, committed_state, submitter, **kwargs):
        super(MetricsFetcher, self).__init__(context, **kwargs)

        self._config = context.config
        self._source = source
        self._ci_state = committed_state
        self._submitter = submitter

        self.sentry_wrapper = context.sentry_wrapper

    def _filter(self, payload):
        '''
        TODO: filter out active workers state
        '''
        return payload

    @gen.coroutine
    def _fetch_and_upload(self):
        if not self._config.metrics.enabled:
            return

        now = time.time()

        payload = yield self._source.fetch({})
        self._ci_state.metrics = self._filter(payload)

        yield self._submitter.post_committed_state()

        elapsed = time.time() - now

        # TODO: update internal metrics
        self.debug('fetching and updating workers stat took {:.3f}s', elapsed)

    @gen.coroutine
    def poll_stats(self):
        while self.should_run():
            try:
                to_sleep = self._config.metrics.poll_interval_sec
                yield gen.sleep(to_sleep)
                yield self._fetch_and_upload()
            except Exception as e:
                self.error('failed to fetch runtime workers stat {}', e)
                yield gen.sleep(self._config.async_error_timeout_sec)


class FeedbackSubmitter(LoggerMixin, MetricsMixin, LoopSentry):
    """Wrapper over unicorn put operation for feedback.

    Provides additional app-wide checks, e.g. was unicorn_feedback configured
    or not.

    """
    def __init__(self, ctx, committed_state, unicorn, async_route_provider,
            **kwargs):
        super(FeedbackSubmitter, self).__init__(ctx, **kwargs)

        self._config = ctx.config
        self._ci_state = committed_state
        self._dumper = Dumper(ctx, unicorn)
        self._async_route_provider = async_route_provider

        self._condition = locks.Condition()

    @gen.coroutine
    def post_committed_state(self):
        """
            Notify of ci_state update if appropriate setup flag is set:

                feedback:
                  unicorn_feedback: true

        """
        if not self._config.feedback.unicorn_feedback:
            self.info('unicorn feedback posting is disabled')
            return

        if not self._ci_state.flushed:
            self._condition.notify()
            self._ci_state.mark_flushed()
        else:
            self.debug('skipping submitting of clean state')

    @gen.coroutine
    def listen_for_committed_state(self):
        while self.should_run():
            try:
                yield self._condition.wait()
                yield self._write(self._ci_state.as_named_dict_ext())
            except Exception as e:
                self.warn('failed to write feedback: {}', e)

    @gen.coroutine
    def _write(self, payload):
        _, dump_to = yield self._async_route_provider()

        self.debug('writing feedback to unicorn path {}', dump_to)
        yield self._dumper.dump(dump_to, payload)


class StateAggregator(LoggerMixin, MetricsMixin, LoopSentry):
    TASK_NAME = 'state_processor'

    def __init__(
            self,
            context,
            node,
            ci_state,
            filter_queue, input_queue, control_queue, submitter,
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

        self.submitter = submitter

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
        '''
            TODO: make wrapper for retries
        '''
        apps_list = set()

        attempts = DEFAULT_RETRY_ATTEMPTS
        while attempts > 0:
            try:
                ch = yield self.node_service.list()
                apps_list = yield ch.rx.get(
                    timeout=self.context.config.api_timeout_by2)
            except gen.TimeoutError as e:
                self.metrics_cnt['list_timeout_error'] += 1
                attempts -= 1
                self.warn(
                    'failed to got apps list, timeout, attempts left {}',
                    attempts
                )
            else:
                break

        raise gen.Return(set(apps_list))

    @gen.coroutine
    def get_info(self, app, flag=0x01):  # 0x01: collect overseer info
        """
            TODO: make wrapper for retries
        """
        info = dict()

        attempts = DEFAULT_RETRY_ATTEMPTS
        while attempts > 0:
            try:
                ch = yield self.node_service.info(app, flag)
                info = yield ch.rx.get(
                    timeout=self.context.config.api_timeout_by2)
            except gen.TimeoutError as e:
                self.metrics_cnt['info_timeout_error'] += 1
                attempts -= 1
                self.warn(
                    'failed to got apps info, timeout {}, attempts left {}',
                    e, attempts
                )
            else:
                break

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
    def runtime_state(self, state, real_state_nonempty, running_apps):
        info = yield self.get_apps_info(running_apps)
        workers_count = self.workers_per_app(info)

        broken_apps, stop_again = set(), set()

        if real_state_nonempty:
            broken_apps = {
                app for app in self.get_broken_apps(info)
                if app in state
            }
            stop_again = {
                app for app in workers_count
                if app not in state and workers_count[app] > 0
            }

        workers_mismatch = self.workers_diff(state, workers_count)

        self.debug('real state non empty {}', real_state_nonempty)
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
        """Get control filter on dispatch main loop startup."""
        # Get control filter on startup
        self.debug('waiting for init control_filter')

        msg = yield self.filter_queue.get()
        self.filter_queue.task_done()

        control_filter = self.update_control_filter(msg.control_filter)

        yield self.control_queue.put(msg)
        self.info('got init control_filter {}', msg.control_filter.as_dict())

        raise gen.Return(control_filter)

    def update_control_filter(self, control_filter):
        self.ci_state.control_filter = control_filter
        return control_filter

    def mark_broken_apps(self, state, broken_apps, state_version):
        """Mark broken apps from state as `failed`.

        Moved out from control loop as it more local to runtime info processing
        and simplify units interconnection a bit, but logically still a
        `control` type procedure.
        """
        now = time.time()
        for app in broken_apps:
            profile = state[app].profile if app in state else ''
            self.ci_state.mark_failed(
                app, profile, state_version, now,
                "app is in broken state")

    def reset_state(self, state, real_state):
        state.clear()
        real_state.clear()

        self.ci_state.reset()
        self.workers_distribution.clear()

    @gen.coroutine
    def dump_feedback_guarded(self):
        try:
            yield self.submitter.post_committed_state()
        except Exception as e:
            self.error('failed to dump feedback record {}', e)

    @gen.coroutine
    def process_loop(self):
        running_apps, broken_apps = set(), set()
        state, prev_state, real_state, state_version = (
            dict(), dict(), dict(), DEFAULT_UNKNOWN_VERSIONS
        )

        last_uuid = None
        control_filter = yield self.get_filter_once()
        no_state_yet = True

        while self.should_run():
            self.status.mark_ok('listenning on incoming queue')

            runtime_reborn = False
            is_state_updated = False

            # Note that uuid is used to determinate was it
            # any incoming state.
            uuid, msg = None, None
            workers_mismatch, stop_again = set(), set()

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
                    no_state_yet = False
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

                    if not real_state:
                        self.info(
                            'empty incoming state, version {}', state_version)

                elif isinstance(msg, ResetStateMessage):
                    runtime_reborn = True
                    no_state_yet = True

                    self.reset_state(state, real_state)
                    self.info('reset state signal')
                elif isinstance(msg, NoStateNodeMessage):
                    self.reset_state(state, real_state)
                    self.info('seems that there is no state node')
                elif isinstance(msg, DumpCommittedState):
                    self.debug('dumping committed state')
                    self.ci_state.mark_dirty()
                    yield self.dump_feedback_guarded()
                    # nothing to do here, skipping next steps.
                    continue
                elif isinstance(msg, ControlFilterMessage):
                    control_filter = \
                        self.update_control_filter(msg.control_filter)

                    self.info(
                        'control_filter updated signal {}',
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
                    ) = yield self.runtime_state(
                        state, bool(real_state), running_apps)

                    #
                    # TODO(mark_broken_apps): could lead to app ban by
                    # scheduler.
                    #
                    self.mark_broken_apps(state, broken_apps, state_version)

                    self.workers_distribution.clear()
                    self.workers_distribution.update(workers_count)

                    yield self.submitter.post_committed_state()

            except Exception as e:
                self.error('failed to get control message with {}', e)
                self.sentry_wrapper.capture_exception()

            # Note that in general following code (up to the end of the
            # method) shouldn't raise.
            if no_state_yet:
                self.info('state not known yet, skipping control iteration')
                continue

            self.status.mark_ok('processing state records')

            update_state_apps_set = state.viewkeys()

            to_run = update_state_apps_set - running_apps
            to_run |= broken_apps

            to_stop = running_apps - update_state_apps_set
            to_hard_stop = broken_apps

            if is_state_updated:  # check for profiles change
                #
                # TODO(Profile update check): temporary disabled, should have
                # more flexible schema profile update in future.
                #
                # to_update = self.make_prof_update_set(prev_state, state)
                #
                to_update = set()

                to_run.update(to_update)
                to_stop.update(to_update)

                prev_state = state
                last_uuid = uuid

                self.debug('profile update list {}', to_update)

            self.info("to_stop apps list {}", to_stop)
            self.info("to_run apps list {}", to_run)
            self.info("to_hard_stop apps list {}", to_hard_stop)

            def should_dispatch():
                return (
                    is_state_updated or to_run or to_stop or
                    workers_mismatch
                )

            if should_dispatch():
                self.status.mark_ok('sending processed state to dispatch')

                # TODO(DispatchMessage): make separate messages for each case.
                yield self.control_queue.put(
                    DispatchMessage(
                        state, real_state,
                        state_version, is_state_updated,
                        to_hard_stop, to_stop, to_run,
                        runtime_reborn,
                        workers_mismatch,
                        stop_again,
                    )
                )

                self.metrics_cnt['to_run_commands'] += len(to_run)
                self.metrics_cnt['to_stop_commands'] += len(to_stop)
            else:
                self.debug('dispatch step was skipped')


class AppsElysium(LoggerMixin, MetricsMixin, LoopSentry):
    """Control life-time of applications based on supplied state."""

    TASK_NAME = 'tasks_dispatch'

    def __init__(self, context, ci_state, node, control_queue, submitter,
            **kwargs):
        super(AppsElysium, self).__init__(context, **kwargs)

        self.context = context
        self.sentry_wrapper = context.sentry_wrapper

        self.ci_state = ci_state

        self.node_service = node
        self.control_queue = control_queue

        self.submitter = submitter
        self.status = context.shared_status.register(AppsElysium.TASK_NAME)

    @gen.coroutine
    def start(self, app, profile, state_version, tm, started=None):
        """Try to start application with specified profile."""
        try:
            ch = yield self.node_service.start_app(app, profile)
            yield ch.rx.get(timeout=self.context.config.api_timeout)
        except Exception as e:
            self.metrics_cnt['errors_start_app'] += 1
            self.status.mark_warn('failed to start application')

            self.error(
                'failed to start app {} {} with err: {}', app, profile, e)

            self.sentry_wrapper.capture_exception(
                message="can't start app",
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
        """Stop/pause application."""
        try:
            ch = yield self.node_service.pause_app(app)
            yield ch.rx.get(timeout=self.context.config.api_timeout)

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
        """Stop application with app.control(0)."""
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

            stopped_by_control.add(app)

            self.metrics_cnt['apps_stopped_by_control'] += 1
            self.info('app {} has been stopped with control', app)
        except Exception as e:  # pragma nocover
            self.error(
                'failed to stop app {} by control with error: {}', app, e)

            self.metrics_cnt['errors_stop_app_by_control'] += 1

            self.sentry_wrapper.capture_exception()
            self.status.mark_warn('failed to stop application by control')
        finally:
            self.ci_state.mark_stopped(app, state_version, tm)

    @gen.coroutine
    def control_with_ack(self, ch, to_adjust):  # pragma nocover
        """Send control and get (dummy) result or exception.

        TODO: tests
        """
        yield ch.tx.write(to_adjust)
        yield ch.rx.get(timeout=self.context.config.api_timeout)

    @gen.coroutine
    def adjust_by_channel(
            self, app, profile, channels_cache, to_adjust, state_version, tm):

        self.debug('control command to {} with {}', app, to_adjust)

        def is_spooling_state(e):
            """Check for spooling state with some heuristics.

            Simple guess for now, should use exception error code and
            category to distinguish among different possible state.

                TODO: check for category
            """
            return \
                isinstance(e, ServiceError) and \
                e.code == INVALID_STATE_ERR_CODE

        attempts = CONTROL_RETRY_ATTEMPTS
        while attempts:
            try:
                ch = yield channels_cache.get_ch(app)
                yield self.control_with_ack(ch, to_adjust)
            except Exception as e:
                if is_spooling_state(e):
                    self.warn(
                        'seems that app {} is in spooling state: {}', app, e)
                    self.ci_state.mark_pending_start(
                        app, to_adjust, profile, state_version, tm)
                    yield channels_cache.close_and_remove([app])
                    break

                attempts -= 1

                error_message = \
                    'send control has been failed for app `{}`, workers {}, ' \
                    'attempts left {}, error: {}' \
                    .format(app, to_adjust, attempts, e)

                self.error(error_message)

                self.status.mark_crit('failed to send control command')
                self.metrics_cnt['errors_of_control'] += 1
                self.sentry_wrapper.capture_exception()

                yield channels_cache.close_and_remove([app])
                yield gen.sleep(DEFAULT_RETRY_EXP_BASE_SEC)

                self.ci_state.mark_failed(
                    app, profile, state_version, tm, error_message)
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

    @gen.coroutine
    def apply_filter(self, channels_cache, control_filter):
        self.info('applying control filter {}', control_filter.as_dict())
        if control_filter.white_list:
            to_retain = \
                filter_apps(channels_cache.apps(), control_filter.white_list)
            yield channels_cache.close_other(to_retain)

    @gen.coroutine
    def stop_hard(self, ch_cache, to_hard_stop, state_version):
        tm = time.time()
        yield ch_cache.close_and_remove(to_hard_stop)
        yield [
            self.slay(app, state_version, tm) for app in to_hard_stop
        ]
        self.metrics_cnt['stopped_hard'] = len(to_hard_stop)

    @gen.coroutine
    def blessing_road(self):
        """Scheduler control commands processing loop."""
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
                    yield self.apply_filter(channels_cache, control_filter)
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
                        'updating fake state, diff {}',
                        command.real_state.viewkeys() -
                        command.state.viewkeys()
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

                    yield channels_cache.close_and_remove_all()
                    self.ci_state.channels_cache_apps = \
                        channels_cache.apps()

                    stopped_by_control.clear()

                    yield self.submitter.post_committed_state()
                    continue

                #
                # Control commands follows
                #
                if self.context.config.stop_apps:  # False by default

                    self.status.mark_ok('stopping apps')
                    tm = time.time()

                    yield [
                        self.stop_by_control(
                            app,
                            command.state_version,
                            tm,
                            channels_cache,
                            stopped_by_control,
                        )
                        for app in command.to_stop
                    ]

                    self.debug('stopping hard, apps {}', command.to_hard_stop)

                    yield self.stop_hard(
                        channels_cache,
                        command.to_hard_stop,
                        command.state_version
                    )

                elif command.to_stop or command.to_hard_stop:
                    self.info(
                        'to_stop list not empty, but stop_apps flag is off, '
                        'skipping `stop apps` stage')

                    # Default is `false`.
                    if self.context.config.pending_stop_in_state:
                        self.debug('mark prohibited to_stop apps in state')
                        pending_stop = command.to_hard_stop | command.to_stop
                        self.mark_pending_stop(
                            pending_stop, command.state_version)
                    else:
                        self.debug('remove prohibited to_stop apps from state')
                        to_remove = command.to_stop | command.to_hard_stop
                        self.ci_state.remove_listed(to_remove)

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
                to_control = set(command.state.viewkeys()) \
                    if command.is_state_updated else started

                stopped_by_control = stopped_by_control - to_control
                self.debug('stopped_by_control {}', stopped_by_control)

                self.metrics_cnt['to_control'] = len(to_control)
                self.metrics_cnt['stopped_by_control'] = \
                    len(stopped_by_control)

                self.debug(
                    'control command will be send for apps: {}', to_control)

                if failed_to_start:  # pragma nocover
                    self.warn(
                        'control command will be skipped for '
                        'failed to start apps: {}',
                        failed_to_start)

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

                self.ci_state.channels_cache_apps = channels_cache.apps()

                self.metrics_cnt['state_updates'] += 1
                self.metrics_cnt['ch_cache_size'] += len(channels_cache)

                yield self.submitter.post_committed_state()

                self.info('state updated')
            except Exception as e:  # pragma nocover
                self.error(
                    'failed to exec command with error {}: {}',
                    type(e).__name__, e)
                self.sentry_wrapper.capture_exception()
                self.status.mark_warn('failed to execute control command')

                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
