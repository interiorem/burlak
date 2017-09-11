
from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop

@gen.coroutine
def start(node, app, profile):
	try:
		print 'starting {} with {}'.format(app, profile)
		ch = yield node.start_app(app, profile)
		yield ch.rx.get()
		print 'starting for {} done'.format(app)
	except Exception as e:
		print 'failed to start app {}, err {}'.format(app, e)

@gen.coroutine
def stop(node, app):
	try:
		print 'stopping {}'.format(app)
		ch = yield node.pause_app(app) # <-- blocked
		print 'stopping ch {}'.format(ch)
		yield ch.rx.get()
		print '{} stopped'.format(app)
	except Exception as e:
		print 'failed to stop app {}, err {}'.format(app, e)

@gen.coroutine
def make_ch(node, app):
	ch = yield node.control(app)
	raise gen.Return(ch)

@gen.coroutine
def control(ch, w):
	print 'writing control {}'.format(ch)
	yield ch.tx.write(w)
	print 'writing done'

@gen.coroutine
def close_ch(ch):
    print 'closing {}'.format(ch)
    yield ch.tx.close()
    print 'tx closed {}'.format(ch)


@gen.coroutine
def loop():
	node = Service('node')

	apps = [
        ('Echo1', 'IsoProcess'),
        ('Echo2', 'IsoProcess'),
        ('Echo3', 'IsoProcess'),
        ('Echo4', 'IsoProcess'),
        # from unsable hosts
        #('faces:1', 'vision'),
		#('licenseplate:8', 'vision'),
		#('superres:2', 'vision'),
	]

	i = 0
	z = 0
	while True:

		i ^= 1
		z += 1

		if i:
			yield [start(node, app, profile) for app, profile in apps]
			chs = yield [make_ch(node, app) for app, _ in apps]

			for y in xrange(10):
				yield [control(c, y+z)  for c in chs]
				yield gen.sleep(1)

			yield [close_ch(c) for c in chs]
		else:
			yield [stop(node, app) for app, _ in apps]

		yield gen.sleep(10)

IOLoop.current().run_sync(loop)
