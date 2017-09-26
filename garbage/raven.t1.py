#!/bin/python

from raven import Client
from raven.transport.tornado import TornadoHTTPTransport

from tornado.ioloop import IOLoop
from tornado import gen

@gen.coroutine
def async_except(cli):
    try:
        1/0
    except ZeroDivisionError as err:
        cli.captureException()
        yield gen.sleep(10)
    except Exception as err:
        print('Error {}'.format(err))

def read_dsn(name):
    with open(name) as f:
        return f.read().rstrip()

cli = Client(
    read_dsn('sentry.dsn'),
    transport=TornadoHTTPTransport,
    release='0.1.1')

IOLoop.current().run_sync(lambda: async_except(cli))
