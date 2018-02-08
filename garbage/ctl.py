from cocaine.services import Service
from cocaine.exceptions import ServiceError
from tornado import gen
from tornado.ioloop import IOLoop

import math

# app = 'yapic:v2-2-36'
app = 'r2d2'

@gen.coroutine
def control(service, externalControl=None):

	delta = 0.1
	ch = yield service.control(app)
	#ch = yield service.control()
	# print 'ch is {}'.format(ch)
	x = delta
	while True:
	# for i in xrange(100):
		ctl = int(5 * math.sin(x) * math.sin(x))
		to_control = externalControl if externalControl is not None else ctl

		val = yield ch.tx.write(to_control)
		res = yield ch.rx.get()

		print 'Send control {} {}'.format(ctl, res)
		yield gen.sleep(2)
		x += delta

node = Service('node')
IOLoop.current().run_sync(lambda: control(node))
