from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop

import json

@gen.coroutine
def get_info(flags):
    node = Service('node')

    ch = yield node.info('ppn', flags)
    info = yield ch.rx.get()

    raise gen.Return(info)


for fl in xrange(16):
    print 'FLAG {}'.format(fl)
    info = IOLoop.current().run_sync(lambda: get_info(fl))
    print json.dumps(info, indent=4)
    print
