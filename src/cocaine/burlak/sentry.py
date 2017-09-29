# TODO: tests
#
import raven
from raven.transport.tornado import TornadoHTTPTransport


class SentryClientWrapper(object):
    '''SentryClientWrapper

    SentryClientWrapper seems redundant as raven doesn't raise exceptions
    (by default) on sentry server failure, but yet it could be used to add
    some additional functionality (e.g logging) and hide raven configuring
    details.
    '''
    def __init__(
            self, logger, dsn, revision, transport=TornadoHTTPTransport,
            **kwargs):

        self.logger = logger

        self.dsn = dsn
        self.revision = revision

        self.client = self._connect(dsn, transport, **kwargs)

    def _connect(self, dsn, transport=TornadoHTTPTransport, **kwargs):
        self.dsn = dsn
        self.transport = TornadoHTTPTransport

        self.client = raven.Client(
            self.dsn,
            transport=self.transport,
            revision=self.revision,
            **kwargs)

        return self.client

    def capture_exception(self, **kwargs):
        try:
            return self.client.captureException(**kwargs)
        except Exception as e:  # pragma nocover
            self.logger.error(
                'failed to send exception info to sentry: {}'.format(e))

    def capture_message(self, message, **kwargs):
        try:
            return self.client.captureMessage(message, **kwargs)
        except Exception as e:  # pragma nocover
            self.logger.error('failed to send info to sentry: {}'.format(e))
