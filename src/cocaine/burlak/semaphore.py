"""Dummy semaphore implementation.

TODO(Semaphore): Coxx API timeout
"""
import random

from tornado import gen

from loop_sentry import LoopSentry
from mixins import *

from collections import namedtuple


class LockHolder(object):
    """Keeper of lock channel.

    Used to bypass lock object over  boundaries.
    """

    def __init__(self, lock=None):
        """LockHolder."""
        self._lock = lock

    @property
    def has_lock(self):
        """Check whether lock is initialized."""
        return self._lock is not None

    @property
    def lock(self):
        """Return lock field."""
        return self._lock

    @lock.setter
    def lock(self, val):
        """Set lock to specified val."""
        self._lock = val


class Semaphore(LoggerMixin, MetricsMixin, LoopSentry):
    """Unicorn based semaphore."""

    Lock = namedtuple('Lock', 'channel')

    def __init__(self, context, unicorn, sharding):
        """Semaphore.

        :param context: application wide context
        :type context: context.Context

        :param unicorn: unicorn service
        :type unicorn: cocaine.services.Service

        :parem sharding: sharding setup
        :type sharding: sharding.ShardingSetup

        """
        super(Semaphore, self).__init__(context)

        self._config = context.config
        self._locks_count = context.config.semaphore.locks_count
        self._locked = set()

        self._sharding = sharding
        self._unicorn = unicorn
        self._locks = {}

        self._sentry_wrapper = context.sentry_wrapper

    @gen.coroutine
    def make_lock_path(self, i):
        """Make unicorn lock path for specified id."""
        _uuid, path = yield self._sharding.get_semaphore_route()
        lock_name = self._config.semaphore.lock_name

        raise gen.Return('{}/{}{}'.format(path, lock_name, i))

    @gen.coroutine
    def try_to_acquire_lock(self, lock_holder):
        """Try to acquire lock."""
        lock_to_try = random.randint(1, self._locks_count)
        lock_path = yield self.make_lock_path(lock_to_try)

        ch = None
        try:
            self.info(
                'will try to acquire semaphore lock {}, path {}',
                lock_to_try, lock_path
            )

            ch = yield self._unicorn.lock(lock_path)
            has_lock = yield ch.rx.get(
                timeout=self._config.semaphore.try_timeout_sec
            )

            if has_lock:
                self.info('got run-lock {}', lock_path)
                lock_holder.lock = Semaphore.Lock(ch)
                return

        except gen.TimeoutError:
            self.debug('timeout on lock, probably it is taken already')
        except Exception as e:
            self.error(
                'error while pocessing lock acquisition {}, error {}',
                lock_path, e
            )
            self._sentry_wrapper.capture_exception()

        self.info('failed to get lock {}, path {}', lock_to_try, lock_path)

        yield self._close_safe(ch)
        yield self.release_lock_holder(lock_holder)

    @gen.coroutine
    def release_lock_holder(self, lock_holder):
        """Close lock channel if it is not None."""
        if lock_holder.has_lock:
            yield self._close_safe(lock_holder.lock.channel)
            lock_holder.lock = None

    @gen.coroutine
    def _close_safe(self, ch):
        try:
            yield ch.tx.close()
        except Exception as e:
            self.error('failed to close lock: {}', e)
