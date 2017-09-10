from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop

import math


LOOP_TO = 2
DELTA = 0.1
AMPH = 10.
ERR_SLEEP = 5


def sin_square(a,x):
    s = math.sin(a)
    return a * s * s


@gen.coroutine
def loop():

    node = Service('node')

    apps = ['Echo{}'.format(i) for i in xrange(1,2)]
    channels = dict()

    #for a in apps:
    #    print ('registering app {}'.format(a))
    #    channels[a] = yield node.control(a)

    ch = yield node.control('Echo1')
    print ('before seq')

    #yield [ch.tx.write(sin_square(AMPH, t * 10)) for t in xrange(1,10)] 
    print ('DONE after seq')
    
    x = 0.0
    while True:
        try:
            print('Sending control.') 

            # yield [channels[a].tx.write(sin_square(AMPH, x)) for a in apps]          
            yield ch.tx.write(sin_square(AMPH, 10))

        except Exception as e:
            print 'error {}'.format(e)
            yield gen.sleep(ERR_SLEEP)
            ch = yield node.control('Echo1')
        else:
            print 'control is done'
            yield gen.sleep(LOOP_TO)
            x += DELTA


IOLoop.current().run_sync(loop) 
