from raven import Client
from raven.transport.tornado import TornadoHTTPTransport


class SentryClientWrapper(object):
    def __init__(
            self, context, dsn, revision, transport=TornadoHTTPTransport):

        self.context = context

        self.__DSN__ = dsn
        self.revision = revision
        self.transport = transport

        self.client = None

    def is_connected(self):
        return self.client is not None

    def connect(self, **kwargs):
        self.client = Client(
            self.__DSN__, transport=self.transport, revision=self.revision)

    def capture_exception(self, **kwargs):
        try:
            if self.is_connected():
                return self.client.captureException(**kwargs)
        except Exception as e:
            self.context.logger.error(
                'failed to send exception info to sentry: {}'.format(e))

    def capture_message(self, **kwargs):
        try:
            if self.is_connected():
                return self.client.captureMessage(**kwargs)
        except Exception as e:
            self.context.logger.error(
                'failed to send info to sentry: {}'.format(e))
