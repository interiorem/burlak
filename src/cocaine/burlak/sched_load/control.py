'''node:control test 
'''
import click

from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop


APP_NAME = 'Echo'
PROFILE = 'DefaultProfile'


@gen.coroutine
def control(times):
    node = Service('node')
    logger = Service('logging')

    try:
        ch = yield node.start_app(APP_NAME, PROFILE)
        result = yield ch.rx.get()

        print('start res {}'.format(result))
        yield logger.emit
    except Exception:
        pass

    control_channel = yield node.control(APP_NAME)
    for i in xrange(0, times):
        # print('running {}'.format(i))
        yield control_channel.tx.write(i % 10)


@click.command()
@click.option(
    '--times', default=10, help='times to post control command')
def main(times):
    print('Starting control test...')
    IOLoop.current().run_sync(lambda: control(times))


if __name__ == '__main__':
    main()
