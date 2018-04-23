#
# TODO: tests
#
from tornado import gen


DEFAULT_UPDATE_TIMEOUT_SEC = 120


def compose_path(prefix, tag, subnode):
    return '{}/{}/{}'.format(prefix, tag, subnode)


class AsyncPathProvider(object):
    def __init__(self, async_method):
        self._async_method = async_method

    @gen.coroutine
    def __call__(self):
        result = yield self._async_method()
        raise gen.Return(result)


class ShardingSetup(object):
    """Provides sharding environment routes.
    """
    def __init__(self, context, uniresis):
        self._ctx = context
        self._uniresis = uniresis
        self._logger = context.logger_setup.logger

    @gen.coroutine
    def get_state_path(self):
        fallback_path = self._ctx.config.uuid_path
        path = yield self._generate_path(fallback_path, 'state_subnode')

        raise gen.Return(path)

    @gen.coroutine
    def get_feedback_path(self):
        fallback_path = self._ctx.config.feedback.unicorn_path
        path = yield self._generate_path(fallback_path, 'feedback_subnode')

        raise gen.Return(path)

    @gen.coroutine
    def get_metrics_path(self):
        fallback_path = self._ctx.config.metrics.path
        path = yield self._generate_path(fallback_path, 'metrics_subnode')

        raise gen.Return(path)

    @gen.coroutine
    def _generate_path(self, fallback_path, path_attribute):
        setup = self._ctx.config.sharding
        tag_key = setup.tag_key

        path = fallback_path
        if setup.enabled:
            tag = yield self._get_dc_tag(tag_key, setup.default_tag)
            path = compose_path(
                setup.common_prefix, tag, getattr(setup, path_attribute))

        uuid = yield self._uniresis.uuid()

        raise gen.Return('{}/{}'.format(path, uuid))

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
            self._logger.info(
                'method uniresis::extra not implemented, '
                'using default tag [%s]', tag
            )

        raise gen.Return(tag)

    @gen.coroutine
    def uuid(self):
        uuid = yield self._uniresis.uuid()
        raise gen.Return(uuid)

    @property
    def uniresis_service_name(self):
        return self._uniresis.service_name
