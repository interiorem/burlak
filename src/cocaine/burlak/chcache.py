# TODO:
#   - clear closed channels on get_ch (separate method?)
#   - tests
#   - metrics
#
from cocaine.services import Service

from collections import namedtuple

from tornado import gen

import time

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
        'service',
        'access_timestamp',
    ])

    def __init__(self):
        self.apps = dict()

    def get(self, app):
        record = self.apps.get(
            app,
            Record(Service(app), 0.0)
        )

        record.access_timestamp = time.time()
        return record

    del remove_old(self, older_then):
        '''Removes all records accessed before `older_then`'''
        to_remove = [
            for app, record in self.apps.iteritems()
            if record.access_time < older_then
        ]

        for app in to_remove:
            del self.apps[app]

        return len(to_remove)


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
            raise gen.Return(True)

        raise gen.Return(False)

    @gen.coroutine
    def add_one(self, app, should_close=False):
        if should_close and app in self.channels:
                self.logger.debug(
                    'ch chache `add_one`: closing ch for {}'.format(app))
                yield close_tx_safe(self.channels[app])

        self.channels[app] = yield self.app_cache.get(app).control()
        raise gen.Return(self.channels[app])

    @gen.coroutine
    def get_ch(self, app, should_close=False):
        if app in self.channels:
            self.logger.debug('ch.cache.get_ch hit for {}'.format(app))
            raise gen.Return(self.channels[app])

        self.logger.debug('ch.cache.get_ch miss for {}'.format(app))
        ch = yield self.add_one(app, should_close)

        raise gen.Return(ch)

    def touch_app_cache(self, older_then):
        return self.app_cache.remove_old(time.time() - older_then)
