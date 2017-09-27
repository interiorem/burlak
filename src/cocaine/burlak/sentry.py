# TODO: tests
#
from raven import Client
from raven.transport.tornado import TornadoHTTPTransport


class SentryClientWrapper(object):
    def __init__(
            self, logger, revision):

        self.logger = logger
        self.revision = revision

        self.dsn = None
        self.transport = None

        self.client = None

    def is_connected(self):
        return self.client is not None

    def connect(self, dsn, transport=TornadoHTTPTransport, **kwargs):
        self.dsn = dsn
        self.transport = TornadoHTTPTransport

        self.client = Client(
            self.dsn,
            transport=self.transport,
            revision=self.revision,
            **kwargs)

    def capture_exception(self, **kwargs):
        try:
            if self.is_connected():
                return self.client.captureException(**kwargs)
        except Exception as e:
            self.logger.error(
                'failed to send exception info to sentry: {}'.format(e))

    def capture_message(self, **kwargs):
        try:
            if self.is_connected():
                return self.client.captureMessage(**kwargs)
        except Exception as e:
            self.logger.error(
                'failed to send info to sentry: {}'.format(e))
