from cocaine.services import Service

from tornado import gen
from tornado import queues

from tornado.ioloop import IOLoop

from datetime import timedelta

import time


@gen.coroutine
def get_children(unicorn, path, nodes_queue):

    ch = yield unicorn.children_subscribe(path)

    while True:
        version, kids = yield ch.rx.get()
        print 'got version {}, kids count {}'.format(version, len(kids))

        nodes_queue.put(kids)


@gen.coroutine
def get_content(unicorn, path, child):
    node_path = '{}/{}'.format(path, child)

    ch = yield unicorn.get(node_path)
    version, content = yield ch.rx.get()

    # print 'ver {} content {}'.format(version, content)

    raise gen.Return((child,content))


@gen.coroutine
def get_nodes(unicorn, nodes_queue):
    kids = list()
    while True:
        try:
            kids = yield nodes_queue.get(timeout=timedelta(seconds=0.5))
            nodes_queue.task_done()
        except gen.TimeoutError as e:
            print 'timeout'

        now = time.time()
        content = yield [get_content(unicorn, path, child) for child in kids]
        content = dict(content)

        print 'content: {}, times {:.2f}'.format(len(content), time.time() - now)

        # print 'item: {}'.format(content['node_1'])
        # print 'kids {}'.format(kids)


unicorn = Service('unicorn')

nodes_queue = queues.Queue()
path = '/darkvoice/nodes.del6'


IOLoop.current().spawn_callback(lambda : get_children(unicorn, path, nodes_queue))
IOLoop.current().spawn_callback(lambda : get_nodes(unicorn, nodes_queue))

IOLoop.current().start()
