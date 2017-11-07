
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

    NA_PROFILE_LABEL = 'n/a'

    def __init__(self):
        self.state = dict()
        self.last_state_version = 0

    def as_dict(self):
        return self.state

    def as_named_dict(self):
        return {app: rec._asdict() for app, rec in self.state.iteritems()}

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
        self.last_state_version = version
