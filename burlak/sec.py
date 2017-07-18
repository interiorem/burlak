#
# Mostly compy-pasted from cocaine-tools
#
from tornado import gen
from cocaine.services import Service
from cocaine.exceptions import CocaineError


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


class WrapperFabric(object):

    @staticmethod
    def make_secure_service(name, mod, client_id, client_secret):
        service = Service(name)

        if mod == 'TVM':
            return SecureService(service, TVM(client_id, client_secret))

        return SecureService(service, Promiscuous())


class SecureService(object):

    def __init__(self, wrapped, secure):
        self._wrapped = wrapped
        self._secure = secure

    @gen.coroutine
    def connect(self, traceid=None):
        yield self._wrapped.connect(traceid)

    def disconnect(self):
        return self._wrapped.disconnect()

    def __getattr__(self, name):
        @gen.coroutine
        def wrapper(*args, **kwargs):
            try:
                kwargs['authorization'] = yield self._secure.fetch_token()
            except Exception as err:
                raise SecureServiceError('failed to fetch secure token: {}'.format(err))
            raise gen.Return((yield getattr(self._wrapped, name)(*args, **kwargs)))
        return wrapper
