from tornado import gen
from tornado.ioloop import IOLoop

from cocaine.services import Service

import click

DEFAULT_PATH = '/darkvoice/orchestration/some_uuid'

@gen.coroutine
def delete_node(node):

    unicorn = Service('unicorn')

    ch = yield unicorn.get(node)
    _, version = yield ch.rx.get()

    if version == -1:
        return
    else:
        ch = yield unicorn.remove(node, version)
        yield ch.rx.get()

@gen.coroutine
def add_node(node, val):
    unicorn = Service('unicorn')

    ch = yield unicorn.get(node)
    _, version = yield ch.rx.get()

    if version == -1:
        ch = yield unicorn.create(node, val)
        yield ch.rx.get()
    else:
        print 'puting data {}'.format(val)
        ch = yield unicorn.put(node, val, version)
        yield ch.rx.get()



@gen.coroutine
def subscribe(node):
    ch = None
    while True:
        try:
            unicorn = Service('unicorn')

            print 'subscribing for {}'.format(DEFAULT_PATH)
            ch = yield unicorn.subscribe(node)

            while True:
                print 'listenning for data...'
                data, version = yield ch.rx.get()
                print 'ver {}, data {}'.format(version, data)

                yield gen.sleep(2)

        except Exception as e:
            print 'error: {} type: {} cat: {}'.format(
                e, e.__class__.__name__, e.category
            )

            if ch is not None:
                ch.tx.close()

@gen.coroutine
def empty():
    raise gen.Return(True)

@click.command()
@click.option('--cmd', default='sub', help='command to exec')
@click.option('--path', default=DEFAULT_PATH, help='path to operate on')
def main(cmd, path):
    to_run = empty

    if cmd == 'sub':
        to_run = lambda: subscribe(path)
    elif cmd == 'del':
        to_run = lambda: delete_node(path)
    elif cmd == 'add':
        to_run = lambda: add_node(path, "Hello, unicorn!")
    else:
        print 'unknown command '
        return

    IOLoop.current().run_sync(to_run)

if __name__ == '__main__':
    main()
