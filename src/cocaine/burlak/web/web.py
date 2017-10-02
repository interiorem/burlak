import os
import time

from tornado import gen
from tornado import web


def make_web_app(
        prefix, uptime, uniresis, committed_state, qs, units, version):
    return web.Application([
        (prefix + r'/state', StateHandler,
            dict(committed_state=committed_state)),
        (prefix + r'/metrics', MetricsHandler,
            dict(queues=qs, units=units)),
        # Used for testing/debugging, not for production, even could
        # cause problem if suspicious code will know node uuid.
        (prefix + r'/info', SelfUUID,
            dict(
                uniresis_proxy=uniresis,
                uptime=uptime,
                version=version)),
    ], debug=False)


class Uptime(object):  # pragma nocover
    def __init__(self):
        self.start_time = time.time()

    def uptime(self):
        return int(time.time() - self.start_time)


class MetricsHandler(web.RequestHandler):
    def initialize(self, queues, units):
        self.queues = queues
        self.units = units

    def get(self):
        metrics = {
            'queues_fill': {
                k: v.qsize() for k, v in self.queues.iteritems()
            },
            'counters': {
                k: v.get_count_metrics() for k, v in self.units.iteritems()
            },
            'system': {
                'load_avg': os.getloadavg(),
            }
        }
        self.write(metrics)
        self.flush()


class StateHandler(web.RequestHandler):
    def initialize(self, committed_state):
        self.committed_state = committed_state

    def get(self):
        last_state = self.committed_state.as_dict()
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


class SelfUUID(web.RequestHandler):
    def initialize(self, uniresis_proxy, uptime, version):
        self.uniresis_proxy = uniresis_proxy
        self.uptime = uptime
        self.version = version

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
        })
        self.flush()
