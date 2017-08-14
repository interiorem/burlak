from tornado import web


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
