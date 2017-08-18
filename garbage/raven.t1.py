#!/bin/python

from raven import Client
from raven.transport.tornado import TornadoHTTPTransport

try:
    cli = Client('https://sentry.test.yandex-team.ru/cocaine', transport=TornadoHTTPTransport)
except Exception as err:
    print('Error {}'.format(err))

try:
    1/0
except ZeroDivisionError as err:
    cli.captureException()

