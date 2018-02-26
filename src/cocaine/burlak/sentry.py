# TODO: tests
#
# Raven install appears to be broken in testing!
#
try:
    import raven
    from raven.transport.tornado import TornadoHTTPTransport
    TRANSPORT = TornadoHTTPTransport
except Exception as e:
    print 'Broken raven package: {}'.format(e)
    TRANSPORT = None


class SentryStub(object):
    def captureException(**kwargs):
        pass

    def captureMessage(message, **kwargs):
        pass


class SentryClientWrapper(object):
    '''SentryClientWrapper

    SentryClientWrapper seems redundant as raven doesn't raise exceptions
    (by default) on sentry server failure, but yet it could be used to add
    some additional functionality (e.g logging) and hide raven configuring
    details.
    '''
    def __init__(
            self, logger, dsn, revision, transport=TRANSPORT, **kwargs):

        self.logger = logger

        self.dsn = dsn
        self.revision = revision

        self.client = self._connect(dsn, transport, **kwargs)

    def _connect(self, dsn, transport=TRANSPORT, **kwargs):
        self.dsn = dsn
        self.transport = transport

        try:
            return raven.Client(
                self.dsn,
                transport=self.transport,
                revision=self.revision,
                **kwargs)
        except NameError:
            print "Raven module wasn't loaded, using 'void' stub!"
            return SentryStub()

    def capture_exception(self, **kwargs):
        try:
            return self.client.captureException(**kwargs)
        except Exception as e:  # pragma nocover
            self.logger.error(
                'failed to send exception info to sentry: {}', e)

    def capture_message(self, message, **kwargs):
        try:
            return self.client.captureMessage(message, **kwargs)
        except Exception as e:  # pragma nocover
            self.logger.error('failed to send info to sentry: {}', e)
