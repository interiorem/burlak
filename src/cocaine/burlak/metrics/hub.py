"""Projetct wide metrics registry."""


class Hub(object):
    """Metrics registry."""

    def __init__(self):
        self._system = {}
        self._applications = {}

    @property
    def system(self):
        return self._system

    @system.setter
    def system(self, s):
        self._system = s

    @property
    def applications(self):
        return self._applications

    @applications.setter
    def applications(self, apps):
        self._applications = apps

    @property
    def metrics(self):
        return {
            'system': self._system,
            'applications': self._applications
        }
