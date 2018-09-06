#
# TODO: tests
#
from tornado import gen


DEFAULT_UPDATE_TIMEOUT_SEC = 120


def compose_path(prefix, tag, subnode):
    return '{}/{}/{}'.format(prefix, tag, subnode)


class ShardingSetup(object):
    """Provides sharding environment routes."""
    def __init__(self, context, uniresis):
        self._ctx = context
        self._uniresis = uniresis
        self._logger = context.logger_setup.logger

    @gen.coroutine
    def get_state_route(self):
        fallback_path = self._ctx.config.uuid_path
        setup = self._ctx.config.sharding
        uuid, path = yield self._get_route(
            setup, fallback_path, setup.state_subnode)

        raise gen.Return((uuid, path))

    @gen.coroutine
    def get_feedback_route(self):
        fallback_path = self._ctx.config.feedback.unicorn_path
        setup = self._ctx.config.sharding
        uuid, path = yield self._get_route(
            setup, fallback_path, setup.feedback_subnode)

        raise gen.Return((uuid, path))

    @gen.coroutine
    def get_semaphore_route(self):
        path = self._ctx.config.semaphore.locks_path
        setup = self._ctx.config.sharding

        if setup.enabled:
            tag = yield self._get_dc_tag(setup.tag_key, setup.default_tag)
            path = \
                compose_path(setup.common_prefix, tag, setup.semaphore_subnode)

        raise gen.Return((None, path))

    @gen.coroutine
    def get_metrics_route(self):
        fallback_path = self._ctx.config.metrics.path
        setup = self._ctx.config.sharding
        uuid, path = yield self._get_route(
            setup, fallback_path, setup.metrics_subnode)

        raise gen.Return((uuid, path))

    @gen.coroutine
    def _get_route(self, setup, fallback_path, subnode):
        tag_key = setup.tag_key

        path = fallback_path
        if setup.enabled:
            tag = yield self._get_dc_tag(tag_key, setup.default_tag)
            path = compose_path(setup.common_prefix, tag, subnode)

        uuid = yield self._uniresis.uuid()
        path = '{}/{}'.format(path, uuid)

        raise gen.Return((uuid, path))

    @gen.coroutine
    def _get_dc_tag(self, tag_key, default):
        tag = default
        try:
            extra = yield self._uniresis.extra()
            if not isinstance(extra, dict):
                raise TypeError('incorrect uniresis extra field type')

            tag = extra.get(tag_key, default)
        except Exception as e:
            # Note: print log, ignore
            self._logger.debug(
                'method uniresis::extra not implemented, '
                'using default tag [%s]', tag
            )

        raise gen.Return(tag)

    @gen.coroutine
    def uuid(self):
        self._logger.debug(
            'retrieving uuid from service %s', self._uniresis.service_name)

        uuid = yield self._uniresis.uuid()
        raise gen.Return(uuid)
