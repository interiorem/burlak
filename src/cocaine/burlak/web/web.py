import os
import resource
import time

from collections import namedtuple

from tornado import gen
from tornado import web


API_V1 = r'v1'

RUsage = namedtuple('RUsage', [
    'maxrss_mb',
    'utime',
    'stime',
])


def make_url(prefix, version, path):
    return '{}/{}/{}'.format(prefix, version, path)


def _get_rusage_partly():
    usage = resource.getrusage(resource.RUSAGE_SELF)

    return RUsage(
        usage.ru_maxrss / 1024.0,  # kB according man page,
        usage.ru_utime,
        usage.ru_stime,
    )


def make_web_app_v1(
        prefix, port, uptime, uniresis, committed_state, qs, units, version):

    app = web.Application([
        (make_url(prefix, API_V1, r'state'), StateHandler,
            dict(committed_state=committed_state)),
        (prefix + r'/state', StateHandler,
            dict(committed_state=committed_state)),
        (make_url(prefix, API_V1, r'failed'), FailedStateHandle,
            dict(committed_state=committed_state)),
        (prefix + r'/failed', FailedStateHandle,
            dict(committed_state=committed_state)),
        (make_url(prefix, API_V1, r'metrics'), MetricsHandler,
            dict(queues=qs, units=units)),

        #
        # Used for testing/debugging, not for production, even could
        # cause problem if suspicious code will know node uuid.
        #
        # Doesn't contain version within path as it could only way to
        # obtain one.
        #
        (prefix + r'/info', SelfUUID,
            dict(
                uniresis_proxy=uniresis,
                uptime=uptime,
                version=version,
                api=API_V1)),
    ], debug=False)

    if port is not None:
        app.listen(port)

    return app


class Uptime(object):  # pragma nocover
    def __init__(self):
        self.start_time = time.time()

    def uptime(self):
        return int(time.time() - self.start_time)


class MetricsHandler(web.RequestHandler):
    def initialize(self, queues, units):
        self.queues = queues
        self.units = units

    @gen.coroutine
    def get(self):

        rusage = _get_rusage_partly()

        metrics = {
            'queues_fill': {
                k: v.qsize() for k, v in self.queues.iteritems()
            },
            'counters': {
                k: v.get_count_metrics() for k, v in self.units.iteritems()
            },
            'system': {
                'load_avg': os.getloadavg(),
                'maxrss_mb': rusage.maxrss_mb,
                'utime': rusage.utime,
                'stime': rusage.stime,
            }
        }

        self.write(metrics)
        self.flush()


class StateHandler(web.RequestHandler):
    def initialize(self, committed_state):
        self.committed_state = committed_state

    @gen.coroutine
    def get(self):
        last_state = self.committed_state.as_named_dict()
        request = self.get_arguments('app')

        result = dict()
        if request:
            for app in request:
                if app in last_state:
                    result[app] = last_state[app]
        else:
            result = last_state

        self.write(result)
        self.flush()


class FailedStateHandle(web.RequestHandler):
    def initialize(self, committed_state):
        self.committed_state = committed_state

    @gen.coroutine
    def get(self):
        version = self.committed_state.version

        failed = [
            app for app, state in self.committed_state.failed.iteritems()
            if state.state_version == version
        ]

        self.write(dict(failed=failed, version=version))
        self.flush()


class SelfUUID(web.RequestHandler):
    def initialize(self, uniresis_proxy, uptime, version, api):
        self.uniresis_proxy = uniresis_proxy
        self.uptime = uptime
        self.version = version
        self.api = api

    @gen.coroutine
    def get(self):
        uuid = ''
        try:
            uuid = yield self.uniresis_proxy.uuid()
        except Exception:  # pragma nocover
            pass

        self.write({
            'uuid': uuid,
            'uptime': self.uptime.uptime(),
            'version': self.version,
            'api': self.api
        })
        self.flush()
