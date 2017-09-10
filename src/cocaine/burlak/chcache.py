from tornado import gen


def close_tx_safe(ch):  # pragma nocover
    '''Close transmitter side of the pipe

    Not really needed in current setup, but may be useful for persistent
    channel of future control implementation.

    '''
    try:
        ch.tx.close()
    except Exception:
        pass


class ChannelsCache(object):

    def __init__(self, logger):
        self.channels = dict()

    @gen.coroutine
    def update(self, node, to_remove, to_add):
        yield self.remove(to_remove)
        yield self.reconnect_all(node)
        yield self.add(to_add)

    @gen.coroutine
    def remove(self, to_remove):
        cnt = 0
        for app in to_remove:
            if app in self.channels:
                close_tx_safe(self.channels[app])
                del self.channels[app]
                cnt += 1

        raise gen.Return(cnt)

    @gen.coroutine
    def reconnect_all(self, node):
        for app in self.channels:
            close_tx_safe(self.channels[app])
            self.channels[app] = yield node.control(app)

    @gen.coroutine
    def add(self, node, to_add):
        for app in to_add:
            if app in self.channels:
                close_tx_safe(self.channels[app])
            self.channels[app] = yield node.control(app)

    @gen.coroutine
    def 
