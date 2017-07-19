#
# Mostly compy-pasted from cocaine-tools
#
from tornado import gen

from cocaine.services import Service
from cocaine.exceptions import CocaineError

from tornado.ioloop import IOLoop


class SecureServiceError(CocaineError):
    pass


class BasicSecurity(object):
    pass


class Promiscuous(BasicSecurity):
    @gen.coroutine
    def fetch_token(self):
        raise gen.Return('')


class TVM(BasicSecurity):

    def __init__(self, client_id, client_secret, name='tvm'):
        self._client_id = client_id
        self._client_secret = client_secret

        self._tvm = Service(name)

    @classmethod
    def ty(self):
        return 'TVM'

    @gen.coroutine
    def fetch_token(self):
        grant_type = 'client_credentials'

        channel = yield self._tvm.ticket_full(self._client_id, self._client_secret, grant_type, {})
        ticket = yield channel.rx.get()

        raise gen.Return(self._make_header(ticket))

    def _make_header(self, ticket):
        return '{} {}'.format(self.ty(), ticket)


class SecureService(object):

    def __init__(self, wrapped, secure, tok_update_sec, loop=IOLoop.current()):
        self._wrapped = wrapped
        self._secure = secure

        self._tok_update_sec = tok_update_sec

        loop.spawn_callback( lambda: self._refresh_token())

    @gen.coroutine
    def _refresh_token(self):
        while True:
            try:
                self._token = yield self._secure.fetch_token()
            finally:
                yield gen.sleep(self._tok_update_sec)

    @gen.coroutine
    def connect(self, traceid=None):
        yield self._wrapped.connect(traceid)

    def disconnect(self):
        return self._wrapped.disconnect()

    def __getattr__(self, name):
        @gen.coroutine
        def wrapper(*args, **kwargs):
            try:
                kwargs['authorization'] = self._token
            except Exception as err:
                raise SecureServiceError('failed to fetch secure token: {}'.format(err))
            raise gen.Return((yield getattr(self._wrapped, name)(*args, **kwargs)))
        return wrapper


class WrapperFabric(object):

    @staticmethod
    def make_secure_service(name, mod, client_id, client_secret, tok_update_to):
        service = Service(name)

        if mod == 'TVM':
            return SecureService(
                service, TVM(client_id, client_secret), tok_update_to)

        return SecureService(service, Promiscuous(), tok_update_to)
