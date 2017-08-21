import click

from cocaine.services import Service

from tornado import gen
from tornado.ioloop import IOLoop


DEFAULT_NODE_PATH = '/some/t1'
DEFAULT_SUBSCRIBER_COUNT = 1000
DEFAULT_STATE_SIZE = 1000
DEFAULT_PUT_TIMES = 1


def generate_state(items):
    return dict(a=1,b=2)


@gen.coroutine
def get_put(unicorn, path, state):
    ch = yield unicorn.get(path)
    _, version = yield ch.rx.get()
    yield unicorn.put(path, state, version)


@gen.coroutine
def do_prod(unicorn, path, items, times):
    state = generate_state(items)

    for _ in xrange(times):
        yield get_put(unicorn, path, state)


@gen.coroutine
def do_subscribe(unicorn, path, subscribers):
    #ch = yield unicorn.subscribe(path)
    #while True:
    for i in xrange(subscribers):
        ch = yield unicorn.subscribe(path)
        print('coroutine num: {} is waiting'.format(i))
        data, version = yield ch.rx.get()
        print('coroutine num: {} get data version: {}'.format(data, version))


@click.command()
@click.argument('cmd')
@click.option('--path', default=DEFAULT_NODE_PATH, help='unicorn node path')
@click.option('--subscribers', default=DEFAULT_SUBSCRIBER_COUNT, help='number of subscribers')
@click.option('--state-size', default=DEFAULT_STATE_SIZE, help='number of items in state')
@click.option('--times', default=DEFAULT_PUT_TIMES, help='times to put state')
def main(cmd, path, subscribers, state_size, times):
    unicorn = Service('unicorn')

    if cmd == 'prod':
        IOLoop.current().run_sync(
            lambda: do_prod(unicorn, path, state_size, times))
    elif cmd == 'subs':
        IOLoop.current().run_sync(
            lambda: do_subscribe(unicorn, path, subscribers))
    else:
        click.secho('unknown command {}'.format(cmd), fg='red')

if __name__ == '__main__':
    main()
