
import time
from collections import namedtuple
from .control_filter import ControlFilter


class Defaults(object):

    NA_PROFILE_LABEL = 'n/a'

    SUCCESS_DESCRIPTION = 'success'
    STOPPED_DESCRIPTION = 'stopped'
    PENDING_STOP_DESCRIPTION = 'pending stop'
    FAILED_DESCRIPTION = 'unknown, study logs'

    INIT_STATE_VERSION = -1


class CommittedState(object):
    """
    State record format:
        <app name> : (<STATE>, <WORKERS COUNT>, <TIMESTAMP>)

        <STATE> - (STARTED|STOPPED|FAILED|PENDING_STOP)
        <TIMESTAMP> - last state update time
    """

    Record = namedtuple('Record', [
        'state',
        'workers',
        'profile',
        'state_version',
        'state_description',
        'time_stamp',
    ])

    IncomingState = namedtuple('IncomingState', [
        'state',
        'version',
        'timestamp'
    ])

    TO_EXPIRE = (
        'FAILED',
        'STOPPED',
        'PENDING_STOP',
    )

    def __init__(self):
        self.in_state = CommittedState.IncomingState(dict(), -1, 0)

        self.ctl_filter = ControlFilter.with_defaults()
        self.state = dict()
        self.last_state_version = Defaults.INIT_STATE_VERSION

        # for now time when version was updated.
        self.updated_timestamp = 0

        self.ch_apps = list()
        # On startup state should be marked as not flushed in order to run
        # unicorn feedback node creation with empty state to inform scheduler
        # that node is online via feedback.
        self._flushed = False

    def as_dict(self):
        return self.state

    def as_named_dict(self):
        return {app: rec._asdict() for app, rec in self.state.iteritems()}

    def as_named_dict_ext(self):
        return dict(
            state = self.as_named_dict(),
            timestamp = self.updated_at,
            version = self.version,
        )

    def reset_output_state(self):
        self.state.clear()
        self.version = Defaults.INIT_STATE_VERSION

    def reset(self):
        self.reset_output_state()
        self.in_state = CommittedState.IncomingState(dict(), -1, 0)
        self.mark_dirty()

    def clear(self):  # pragma nocover
        '''Alias for reset'''
        self.reset()

    @property
    def flushed(self):
        return self._flushed

    def mark_flushed(self):
        self._flushed = True

    def mark_dirty(self):
        self._flushed = False

    def mark_running(self, app, workers, profile, state_version, tm):
        self.state.update(
            {
                app: CommittedState.Record(
                    'STARTED',
                    workers,
                    profile,
                    state_version,
                    Defaults.SUCCESS_DESCRIPTION,
                    int(tm))
            })

        self.mark_dirty()

    def mark_failed(self, app, profile, state_version, tm, reason=None):
        if reason is None:
            reason = Defaults.FAILED_DESCRIPTION

        self.state.update(
            {
                app: CommittedState.Record(
                    'FAILED',
                    0,
                    profile,
                    state_version,
                    reason,
                    int(tm))
            })

        self.mark_dirty()


    def mark_stopped(self, app, state_version, tm):
        _, workers, profile, _, description, _ = self.state.get(
            app,
            CommittedState.Record(
                '',
                0,
                Defaults.NA_PROFILE_LABEL,
                0,
                Defaults.STOPPED_DESCRIPTION,
                0
            )
        )

        self.state.update(
            {
                app: CommittedState.Record(
                    'STOPPED',
                    workers,
                    profile,
                    state_version,
                    description,
                    int(tm)),
            })

        self.mark_dirty()

    def remove_old_stopped(self, expire_span):
        self.remove_old_records(expire_span, ('STOPPED',))

    def remove_expired(self, expire_span):
        '''Removes all non-RUNNING records
        '''
        self.remove_old_records(expire_span, self.TO_EXPIRE)

    def remove(self, app):
        self.state.pop(app, {})

    def remove_listed(self, apps):
        for a in apps:
            self.remove(a)

    def remove_old_records(self, expire_span, in_state):
        now = time.time()
        to_remove = [
            app for app, last_state in self.state.iteritems()
            if last_state.state in in_state
            and last_state.time_stamp < now - expire_span
        ]

        for app in to_remove:
            del self.state[app]

    @property
    def failed(self):
        return {
            app: state for app, state in self.state.iteritems()
            if state.state == 'FAILED'
        }

    @property
    def version(self):
        return self.last_state_version

    @version.setter
    def version(self, version):
        self.updated_at = time.time()
        self.last_state_version = version

    @property
    def incoming_state(self):
        '''Used mostly for debugging.
        '''
        return self.in_state._asdict()

    def set_incoming_state(self, state, version, ts=None):
        if ts is None or not isinstance(ts, (int, long, float)):
            ts = time.time()

        # Convert per app records to dictionaries.
        state = {
            app: val._asdict()
            for app, val in state.iteritems()
        }

        self.in_state = CommittedState.IncomingState(state, version, int(ts))

    @property
    def control_filter(self):
        return self.ctl_filter

    @control_filter.setter
    def control_filter(self, ctl_filter):
        self.ctl_filter = ctl_filter

    @property
    def channels_cache_apps(self):
        return self.ch_apps

    @channels_cache_apps.setter
    def channels_cache_apps(self, ch_apps):
        self.ch_apps = ch_apps

    @property
    def updated_at(self):
        return self.updated_timestamp

    @updated_at.setter
    def updated_at(self, ts):
        self.updated_timestamp = int(ts)

    def mark_pending_stop(self, app, state_version, tm):
        _, workers, profile, _, _, _ = self.state.get(
            app,
            CommittedState.Record(
                '',
                0,
                Defaults.NA_PROFILE_LABEL,
                0,
                Defaults.PENDING_STOP_DESCRIPTION,
                0
            )
        )

        self.state.update(
            {
                app: CommittedState.Record(
                    'PENDING_STOP',
                    workers,
                    profile,
                    state_version,
                    Defaults.PENDING_STOP_DESCRIPTION,
                    int(tm))
            }
        )

    def running_apps_count(self):
        return sum(
            1 for _ in self.generate_subset(lambda x: x.state == 'STARTED')
        )

    def workers_count(self):
        try:
            return sum(
                int(record.workers)
                for _, record in self.generate_subset(
                    lambda x: x.state == 'STARTED'
                )
            )
        except Exception:  # pragma nocover
            return 0

    def generate_subset(self, predicate):
        for app, record in self.as_dict().iteritems():
            if predicate(record):
                yield (app, record)
