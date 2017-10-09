# TODO:
#   - clear closed channels on get_ch (separate method?)
#   - tests
#   - metrics
#
import time

from collections import namedtuple

from cocaine.services import Service

from tornado import gen


@gen.coroutine
def close_tx_safe(ch, logger=None):  # pragma nocover
    '''Close transmitter side of the pipe

    Not really needed in current setup, but may be useful for persistent
    channel of future control implementation.
    '''
    try:
        yield ch.tx.close()
    except Exception as e:
        if logger:
            logger.error('failed to close channel, err {}'.format(e))


class _AppsCache(object):
    '''App services cache with handy control channel creation method
    '''

    Record = namedtuple('Record', [
        'application',
        'touch_timestamp',
    ])

    def __init__(self, logger):
        self.logger = logger
        self.apps = dict()

    @gen.coroutine
    def make_control_ch(self, app):
        ch = yield self._get(app).control()
        raise gen.Return(ch)

    def _get(self, app):
        now = time.time()
        app_service = Service(app) \
            if app not in self.apps else self.apps[app].application

        self.apps[app] = _AppsCache.Record(app_service, now)
        return app_service

    def remove_old(self, expire_from_now):
        '''Removes all records accessed before `older_then`'''
        older_then = time.time() - expire_from_now

        to_remove = [
            app
            for app, record in self.apps.iteritems()
            if record.touch_timestamp < older_then
        ]

        self.logger.debug(
            'removing elder applications from cach: {}'
            .format(to_remove))

        self.remove(to_remove)
        return len(to_remove)

    def remove(self, to_remove):
        for app in to_remove:
            if app in self.apps:
                del self.apps[app]

    def update(self, to_add, expire_from_now=None):
        if expire_from_now is not None:
            self.remove_old(expire_from_now)

        for app in to_add:
            self._get(app)


class ChannelsCache(object):

    DEFAULT_UPDATE_ATTEMPTS = 3
    DEFAULT_UPDATE_TIMEOUT_SEC = 5

    def __init__(self, logger):
        self.channels = dict()
        self.logger = logger
        self.app_cache = _AppsCache(logger)

    @gen.coroutine
    def close_and_remove(self, to_rem):
        cnt = 0
        for app in to_rem:
            result = yield self.close_one(app)
            if result:
                cnt += 1

        raise gen.Return(cnt)

    @gen.coroutine
    def close_and_remove_all(self):
        yield self.close_and_remove(list(self.channels.iterkeys()))

    @gen.coroutine
    def close_one(self, app):
        if app in self.channels:
            self.logger.debug('removing from cache {}'.format(app))
            yield close_tx_safe(self.channels[app])
            del self.channels[app]
            self.app_cache.remove([app])
            raise gen.Return(True)

        raise gen.Return(False)

    @gen.coroutine
    def add_one(self, app, should_close=False):
        if should_close and app in self.channels:
            self.logger.debug(
                'ch chache `add_one`: closing ch for {}'.format(app))
            yield close_tx_safe(self.channels[app])

        self.channels[app] = yield self.app_cache.make_control_ch(app)
        raise gen.Return(self.channels[app])

    @gen.coroutine
    def get_ch(self, app, should_close=False):
        if app in self.channels:
            self.logger.debug('ch.cache.get_ch hit for {}'.format(app))
            raise gen.Return(self.channels[app])

        self.logger.debug('ch.cache.get_ch miss for {}'.format(app))
        ch = yield self.add_one(app, should_close)

        raise gen.Return(ch)

    @gen.coroutine
    def update(self, to_close, to_add, expire_from_now=None):
        yield self.close_and_remove(to_close)
        self.app_cache.update(to_add, expire_from_now)
