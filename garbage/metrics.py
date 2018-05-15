import time


from cocaine.services import Service

from tornado.ioloop import IOLoop
from tornado import gen


TO_SLEEP = 5

service_name = 'metrics'

@gen.coroutine
def metrics(service):

    while True:

        print 'Getting metrics'
        start = time.time()
        ch = yield service.fetch()
        data = yield ch.rx.get()

        # print 'request took {:.3f} seconds'.format(time.time() - start)
        data = json.loads(data)
        data = json.dumps(data, indent=4, sort_keys=True)

        print 'data is {}'.format(data)

        yield gen.sleep(TO_SLEEP)



IOLoop.current().run_sync(lambda: metrics(Service(service_name)))
