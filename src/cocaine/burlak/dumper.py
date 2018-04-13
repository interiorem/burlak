from tornado import gen

DEFAULT_ATTEMPTS = 5
DEFAULT_RETRY_TIMEOUT_SEC = 1.0


class Dumper(object):
    '''Dumper stores provided payload using unicorn service
    '''
    def __init__(self, context, unicorn):
        self.unicorn = unicorn
        self.context = context
        self.logger = context.logger_setup.logger

    @gen.coroutine
    def _upload(self, path, payload, _ephemeral):
        """
        TODO: ephemeral nodes aren't supported by unicorn service API yet.
        """
        api_timeout = self.context.config.api_timeout

        ch = yield self.unicorn.get(path)
        _, version = yield ch.rx.get(timeout=api_timeout)

        if version == -1:
            self.logger.debug('no unicorn node, creating {}'.format(path))

            ch = yield self.unicorn.create(path, payload)
            yield ch.rx.get(timeout=api_timeout)

            version = 0
        else:
            self.logger.debug(
                'writing unicorn node {}, version {}'.format(path, version))

            ch = yield self.unicorn.put(path, payload, version)
            version = yield ch.rx.get(timeout=api_timeout)

        raise gen.Return(version)

    @gen.coroutine
    def dump(self, path, payload, ephemeral=True):
        """dumps provided `payload` at `path` node

        TODO: ephemeral type not exposed from unicorn API.

        returns: version of written payload
        """
        attempts = DEFAULT_ATTEMPTS
        version = -1

        while attempts > 0:
            try:
                version = yield self._upload(path, payload, ephemeral)
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

                raise gen.Return(version)
