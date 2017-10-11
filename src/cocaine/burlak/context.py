from collections import namedtuple


LoggerSetup = namedtuple('LoggerSetup', [
    'logger',
    'dup_to_console',
])


'''Application wide internal state holders, handlers, e.g.:
- config
- logger
- sentry_wrapper
...
'''
Context = namedtuple('Context', [
    'logger_setup',
    'config',
    'revision',
    'sentry_wrapper',
    'shared_status',
])
