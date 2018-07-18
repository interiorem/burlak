"""Project wide metrics registry."""


class Hub(object):
    """Metrics registry."""

    def __init__(self):
        self.system = {}
        self.applications = {}

    @property
    def metrics(self):
        return {
            'system': self.system,
            'applications': self.applications
        }
