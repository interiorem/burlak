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
    Record = namedtuple('Record', [
        'application',
        'make_timestamp',
    ])

    def __init__(self):
        self.apps = dict()

    @gen.coroutine
    def make_control_ch(self, app):
        ch = yield self.get(app).control()
        raise gen.Return(ch)

    def get(self, app):
        return self.apps.get(
            app, _AppsCache.Record(Service(app), time.time())
        ).application

    def remove_old(self, older_then):
        '''Removes all records accessed before `older_then`'''
        to_remove = [
            app
            for app, record in self.apps.iteritems()
            if record.access_time < older_then
        ]

        self.remove(to_remove)
        return len(to_remove)

    def remove(self, to_remove):
        for app in to_remove:
            if app in self.apps:
                del self.apps[app]

    def keep(self, to_keep):
        '''Remove apps not in `to_keep` list'''
        self.remove(set(self.apps.iterkeys()) - set(to_keep))

    def update(self, apps):
        for app in apps:
            self.get(app)


#
# TODO: exception handling, retries on error
#
class ChannelsCache(object):

    DEFAULT_UPDATE_ATTEMPTS = 3
    DEFAULT_UPDATE_TIMEOUT_SEC = 5

    def __init__(self, logger):
        self.channels = dict()
        self.logger = logger
        self.app_cache = _AppsCache()

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
    def update(self, state, to_remove):
        yield self.close_and_remove(to_remove)
        self.app_cache.update(state.iterkeys())
