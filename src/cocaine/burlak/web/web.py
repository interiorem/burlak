import time
from collections import namedtuple

from tornado import gen
from tornado import web

from ..helpers import flatten_dict


API_V1 = r'v1'


WebOptions = namedtuple('WebOptions', [
    'prefix',
    'port',
    'uptime',
    'uniresis',
    'committed_state',
    'metrics_gatherer',
    'qs',
    'units',
    'workers_distribution',
    'white_list',
    'version',
])


def make_url(prefix, version, path):
    return '{}/{}/{}'.format(prefix, version, path)


def make_web_app_v1(opts):
    '''make_web_app_v1

    For legacy API some handlers are exposed with API version part and without

    '''
    app = web.Application([
        (make_url(opts.prefix, API_V1, r'state'), StateV1Handler,
            dict(committed_state=opts.committed_state)),
        (make_url(opts.prefix, API_V1, r'incoming_state'), IncomingStateHandle,
            dict(committed_state=opts.committed_state)),
        (opts.prefix + r'/state', StateHandler,
            dict(committed_state=opts.committed_state)),
        (make_url(opts.prefix, API_V1, r'failed'), FailedStateHandle,
            dict(committed_state=opts.committed_state)),
        (make_url(opts.prefix, API_V1, r'white_list'), WhiteList,
            dict(white_list=opts.white_list)),
        (make_url(opts.prefix, API_V1, r'distribution(/?[^/]*)'),
            WorkersDistribution,
            dict(workers_distribution=opts.workers_distribution)),
        (opts.prefix + r'/failed', FailedStateHandle,
            dict(committed_state=opts.committed_state)),
        (
            make_url(opts.prefix, API_V1, r'metrics'),
            MetricsHandler,
            dict(
                committed_state=opts.committed_state,
                queues=opts.qs,
                units=opts.units,
                metrics_gatherer=opts.metrics_gatherer,
            )
        ),

        #
        # Used for testing/debugging, not for production, even could
        # cause problem if suspicious code will know node uuid.
        #
        # Doesn't contain version within path as it could only way to
        # obtain one.
        #
        (opts.prefix + r'/info', SelfUUID,
            dict(
                uniresis_proxy=opts.uniresis,
                uptime=opts.uptime,
                version=opts.version,
                api=API_V1)),
    ], debug=False)

    if opts.port is not None:
        app.listen(opts.port)

    return app


class Uptime(object):  # pragma nocover
    def __init__(self):
        self.start_time = time.time()

    def uptime(self):
        return int(time.time() - self.start_time)


class MetricsHandler(web.RequestHandler):
    def initialize(self, committed_state, queues, units, metrics_gatherer):
        self.committed_state = committed_state
        self.queues = queues
        self.units = units
        self.metrics_gatherer = metrics_gatherer

    @gen.coroutine
    def get(self):

        flatten = self.get_argument('flatten', default=None)

        metrics = {
            'queues_fill': {
                k: v.qsize() for k, v in self.queues.iteritems()
            },
            'counters': {
                k: v.get_count_metrics() for k, v in self.units.iteritems()
            },
            'system': self.metrics_gatherer.as_dict(),
            'committed_state': {
                'running_apps_count':
                    self.committed_state.running_apps_count(),
                'workers_count': self.committed_state.workers_count(),
            },
        }

        if flatten is not None:
            metrics = dict(flatten_dict(metrics))

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


class StateV1Handler(web.RequestHandler):
    def initialize(self, committed_state):
        self.committed_state = committed_state

    @gen.coroutine
    def get(self):
        last_state = self.committed_state.as_named_dict()
        last_version = self.committed_state.version
        last_timestamp = self.committed_state.updated_at

        request = self.get_arguments('app')

        result = dict()
        if request:
            for app in request:
                if app in last_state:
                    result[app] = last_state[app]
        else:
            result = last_state

        self.write(
            dict(
                state=result,
                version=last_version,
                timestamp=last_timestamp
            )
        )

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


class IncomingStateHandle(web.RequestHandler):
    '''Viewer for last state from scheduler

    Used mostly for debugging

    '''
    def initialize(self, committed_state):
        self.committed_state = committed_state

    @gen.coroutine
    def get(self):
        self.write(self.committed_state.incoming_state)


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

        self.write(
            dict(
                uuid=uuid,
                uptime=self.uptime.uptime(),
                version=self.version,
                api=self.api
            )
        )

        self.flush()


class WorkersDistribution(web.RequestHandler):

    def initialize(self, workers_distribution):
        self.workers_distribution = workers_distribution

    @gen.coroutine
    def get(self, subset=None):
        result = result = self.workers_distribution

        if subset == '/none':
            result = {
                app: workers
                for app, workers in self.workers_distribution.iteritems()
                if not workers
            }
        elif subset == '/some':
            result = {
                app: workers
                for app, workers in self.workers_distribution.iteritems()
                if workers
            }

        self.write(result)


class WhiteList(web.RequestHandler):
    def initialize(self, white_list):
        self.white_list = white_list

    @gen.coroutine
    def get(self):
        self.write(dict(white_list=self.white_list))
