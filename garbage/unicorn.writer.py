from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop

import msgpack

PFX = '/dark'+ \
    'voice'

@gen.coroutine
def load_from_storage(service, name):
    ch = yield service.read('profiles', name)
    data = yield ch.rx.get()

    raise gen.Return(msgpack.loads(data))

@gen.coroutine
def upload_to_unicorn(service, path, data):
    ch = yield service.get(path)
    old_data, version = yield ch.rx.get()

    # print 'current data {}'.format(old_data)
    print 'current version {}'.format(version)

    if version == -1:
        ch = yield service.create(path, data)
        ok = yield ch.rx.get()
    else:
        ch = yield service.put(path, data, version)
        ok = yield ch.rx.get()
        #print 'wrote: {}'.format(data)


@gen.coroutine
def upload_to_unicorn_dict(service, path, d):

    def make_path(path, name):
        return '{}/{}'.format(path, name)

    for k, v in d.iteritems():
        p = make_path(path, k)

        print '\tstoring to {}'.format(p)
        print '\tdata {}'.format(v)

        yield upload_to_unicorn(service, p, v)


path = PFX + '/cfg/develop/runlist/default'
data = dict(('Echo' + str(i), 'IsoProcess') for i in xrange(5))

unicorn = Service('unicorn')
IOLoop.current().run_sync(lambda : upload_to_unicorn(unicorn, path, data))


storage = Service('storage')
name = 'IsoProcess'
profile = IOLoop.current().run_sync(lambda : load_from_storage(storage, name))


path = PFX + '/cfg/develop/profile'
content = IOLoop.current().run_sync(lambda : upload_to_unicorn_dict(unicorn, path, {name: profile}))
