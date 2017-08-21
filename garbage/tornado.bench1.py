from tornado import gen
from tornado.ioloop import IOLoop

import timeit


@gen.coroutine
def dummy(i):
    raise gen.Return(i + 1)


@gen.coroutine
def for_range(times):
    for i in xrange(times):
        yield dummy(i)


@gen.coroutine
def list_comp(times):
     yield [dummy(i) for i in xrange(times)]


def run(func):
    IOLoop.current().run_sync(func)

# Timings:
#
# list: 52.1057009697
# for: 59.2660291195
#
def main():
    print(timeit.timeit('run(lambda: list_comp(10000))',
        setup='from __main__ import run, list_comp', number=1000))
    print(timeit.timeit('run(lambda: for_range(10000))',
        setup='from __main__ import run, for_range', number=1000))


if __name__ == '__main__':
    main()
