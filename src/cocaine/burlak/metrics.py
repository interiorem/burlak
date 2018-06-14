from tornado import gen

class BaseMetrics(object):
    """Basic workers metrics provider
    """
    def __init__(self, service_name):
        self._service_name = service_name

    @gen.coroutine
    def fetch(self, _query):
        raise gen.Return(dict())
