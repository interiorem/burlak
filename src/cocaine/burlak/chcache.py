# TODO:
#   - clear closed channels on get_ch (separate method?)
#   - tests
#   - metrics
#
import time

from collections import namedtuple

from cocaine.services import Service

from itertools import ifilter

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
            logger.error('failed to close channel {}, err {}'
            .format(ch, e))


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

    # TODO: unused, even deprecated, remove someday
    def remove_old(self, expire_from_now):
        '''Removes all records accessed before `older_then`'''
        older_then = time.time() - expire_from_now

        expired = [
            app
            for app, record in self.apps.iteritems()
            if record.touch_timestamp < older_then
        ]

        self.logger.debug(
            'removing expired applications from cache: {}'
            .format(expired))

        self.remove(expired)
        return expired

    def remove(self, to_remove):
        self.logger.debug(
            'trying to remove from apps cache {}'
            .format(to_remove))

        for app in to_remove:
            if app in self.apps:
                del self.apps[app]


class ChannelsCache(object):
    def __init__(self, logger):
        self.channels = dict()
        self.logger = logger
        self.app_cache = _AppsCache(logger)

    @gen.coroutine
    def close_and_remove(self, to_remove):
        cnt = 0
        for app in ifilter(lambda a: a in self.channels, to_remove):
            self.logger.debug('removing from cache {}'.format(app))
            yield close_tx_safe(self.channels[app], self.logger)
            del self.channels[app]
            self.app_cache.remove([app])

        raise gen.Return(cnt)

    @gen.coroutine
    def close_and_remove_all(self):
        yield self.close_and_remove(self.channels.iterkeys())

    @gen.coroutine
    def add_one(self, app, should_close=False):
        if should_close and app in self.channels:
            self.logger.debug(
                'ch chache `add_one`: closing ch for {}'.format(app))
            yield close_tx_safe(self.channels[app], self.logger)

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
