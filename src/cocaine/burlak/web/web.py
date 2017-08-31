import os
import time

from tornado import gen
from tornado import web


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
        self.write(self.committed_state.as_dict())
        self.flush()


class SelfUUID(web.RequestHandler):
    def initialize(self, uniresis_stub, uptime):
        self.uniresis_stub = uniresis_stub
        self.uptime = uptime

    @gen.coroutine
    def get(self):
        uuid = yield self.uniresis_stub.uuid()

        self.write({
            'uuid': uuid,
            'uptime': self.uptime.uptime(),
        })
        self.flush()
