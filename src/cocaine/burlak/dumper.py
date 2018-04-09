from tornado import gen

DEFAULT_ATTEMPTS = 5
DEFAULT_RETRY_TIMEOUT_SEC = 1.0


class Dumper(object):

    def __init__(self, context, unicorn):
        self.unicorn_service = unicorn
        self.context = context
        self.logger = context.logger_setup.logger

    @gen.coroutine
    def _upload(self, path, payload, _ephemeral):
        '''
        TODO: ephemeral nodes aren't supported by unicorn service API yet.
        '''
        api_timeout = self.context.config.api_timeout

        ch = yield self.unicorn_service.get(path)
        _, version = yield ch.rx.get(timeout=api_timeout)

        if version == -1:
            self.logger.debug('no unicorn node, creating {}'.format(path))

            ch = yield self.unicorn.create(path, payload)
            yield ch.rx.get(timeout=api_timeout)
            version = 0

        self.logger.debug(
            'writing unicorn node {}, version {}'.format(path, version))

        ch = yield self.unicorn_service.put(path, version)
        version = yield ch.rx.get(timeout=api_timeout)
        self.logger.debug(
            "value for path {} was written with version {}"
            .format(path, version))

    @gen.coroutine
    def dump(self, path, payload, ephemeral=True):
        attempts = DEFAULT_ATTEMPTS
        while attempts > 0:
            try:
                yield self._upload(path, payload, ephemeral)
            except Exception as e:
                attempts -= 1

                self.logger.error(
                    'failed to write to unicorn, path: '
                    '{}, attempts: {}, error: {}'
                    .format(path, attempts, e))

                yield gen.sleep(DEFAULT_RETRY_TIMEOUT_SEC)
            else:
                self.logger.info(
                    'wrote to unicorn path: {}, version {}'
                    .format(path, version))

                raise gen.Return(True)
