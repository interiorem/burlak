
class MetricsException(Exception):
    def __init__(self, message):
        self._message = message

    def __str__(self):
        return 'metrics_error("{}")'.format(self._message)
