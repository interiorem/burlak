
import time
from collections import namedtuple


class CommittedState(object):
    """
    State record format:
        <app name> : (<STATE>, <WORKERS COUNT>, <TIMESTAMP>)

        <STATE> - (STARTED|STOPPED)
        <TIMESTAMP> - last state update time
    """

    Record = namedtuple('Record', [
        'state',
        'workers',
        'profile',
        'state_version',
        'time_stamp',
    ])

    IncomingState = namedtuple('IncomingState', [
        'state',
        'version',
        'timestamp'
    ])

    NA_PROFILE_LABEL = 'n/a'

    def __init__(self):
        self.in_state = CommittedState.IncomingState(dict(), -1, 0)

        self.state = dict()
        self.last_state_version = 0
        # for now time when version was updated.
        self.updated_timestamp = 0

    def as_dict(self):
        return self.state

    def as_named_dict(self):
        return {app: rec._asdict() for app, rec in self.state.iteritems()}

    def reset(self):
        self.state.clear()

    def mark_running(self, app, workers, profile, state_version, tm):
        self.state.update(
            {
                app: CommittedState.Record(
                    'STARTED',
                    workers,
                    profile,
                    state_version,
                    int(tm))
            })

    def mark_failed(self, app, profile, state_version, tm):
        self.state.update(
            {
                app: CommittedState.Record(
                    'FAILED',
                    0,
                    profile,
                    state_version,
                    int(tm))
            })

    def mark_stopped(self, app, state_version, tm):
        _, workers, profile, _, _ = self.state.get(
            app, CommittedState.Record('', 0, self.NA_PROFILE_LABEL, 0, 0))

        self.state.update(
            {
                app: CommittedState.Record(
                    'STOPPED',
                    workers,
                    profile,
                    state_version,
                    int(tm)),
            })

    def remove_old_stopped(self, expire_span):
        now = time.time()
        to_remove = [
            app for app, last_state in self.state.iteritems()
            if last_state.state == 'STOPPED'
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

        print 'IN STATE {}'.format(self.in_state)

    @property
    def updated_at(self):
        return self.updated_timestamp

    @updated_at.setter
    def updated_at(self, ts):
        self.updated_timestamp = int(ts)
