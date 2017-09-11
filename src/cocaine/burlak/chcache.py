# TODO:
#   - clear closed channels on get_ch (separate method?)
#   - tests
#   - metrics
#
from tornado import gen


@gen.coroutine
def close_tx_safe(ch):  # pragma nocover
    '''Close transmitter side of the pipe

    Not really needed in current setup, but may be useful for persistent
    channel of future control implementation.

    '''
    try:
        yield ch.tx.close()
    except Exception:
        pass


#
# TODO: exception handling, retries on error
#
class ChannelsCache(object):

    DEFAULT_UPDATE_ATTEMPTS = 3
    DEFAULT_UPDATE_TIMEOUT_SEC = 5

    def __init__(self, logger, node):
        self.channels = dict()
        self.node = node
        self.logger = logger

    @gen.coroutine
    def update(self, to_remove, to_add):
        attempts = ChannelsCache.DEFAULT_UPDATE_ATTEMPTS

        while attempts:
            try:
                yield self.close_and_remove(to_remove)
                self.logger.info('removed from ch cache {}'.format(to_remove))

                if to_remove:
                    yield self.reconnect_all()
                    self.logger.info('cache: all control channels reconnected')

                yield [self.add_one(app) for app in to_add]
                self.logger.info('added to cache {}'.format(to_add))
            except Exception as e:
                attempts -= 1

                self.logger.error(
                    'failed to update channels cache {} attempts left {}'
                    .format(e, attempts))

                assert attempts >= 0
                yield gen.sleep(ChannelsCache.DEFAULT_UPDATE_TIMEOUT_SEC)
            else:
                break

    @gen.coroutine
    def close_and_remove(self, to_rem):
        cnt = 0
        for app in to_rem:
            if app in self.channels:
                self.logger.debug('removing from cache {}'.format(app))
                yield close_tx_safe(self.channels[app])
                del self.channels[app]
                cnt += 1

        raise gen.Return(cnt)

    @gen.coroutine
    def close_all(self):
        yield self.close_and_remove(channels.iterkeys())

    @gen.coroutine
    def reconnect_all(self):
        for app in self.channels:
            self.logger.debug('reconnecting control to {}'.format(app))
            close_tx_safe(self.channels[app])
            self.channels[app] = yield self.node.control(app)
            self.logger.info('reconnected control for app {}'.format(app))

    @gen.coroutine
    def add_one(self, app, should_close=False):
        if should_close and app in self.channels:
                self.logger.debug(
                    'ch chache `add_one`: closing ch for {}'
                    .format(app))
                close_tx_safe(self.channels[app])

        self.channels[app] = yield self.node.control(app)
        raise gen.Return(self.channels[app])

    @gen.coroutine
    def get_ch(self, app):
        if app in self.channels:
            self.logger.debug('ch.cache.get_ch hit for {}'.format(app))
            raise gen.Return(self.channels[app])

        self.logger.debug('ch.cache.get_ch hit for {}'.format(app))

        ch = yield self.add_one(app)
        raise gen.Return(ch)
