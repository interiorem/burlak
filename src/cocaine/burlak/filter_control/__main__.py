import click
from cocaine.services import Service
from ..helpers.secadaptor import SecureServiceFabric
from ..mokak.mokak import SharedStatus
from tornado.ioloop import IOLoop
from tornado import gen
from ..config import Config


DEFAULT_FILTER_PATH = '/darkvoice/control_filter'


def get_secure_unicorn():
    config = Config(SharedStatus())
    config.update()

    return SecureServiceFabric.make_secure_adaptor(
        Service('unicorn'), *config.secure, endpoints=config.locator_endpoints)

@gen.coroutine
def update(unicorn, path, record):
    print 'writing to path {}, record {}'.format(path, record)

    ch = yield unicorn.get(path)
    _, version = yield ch.rx.get()

    if version == -1:
        print 'creating node...'
        yield unicorn.create(path, {})
        version = 0

    ch = yield unicorn.put(path, record, version)
    _, (result, _) = yield ch.rx.get()


@gen.coroutine
def get(unicorn, path):
    ch = yield unicorn.get(path)
    data, version = yield ch.rx.get()

    if version == -1:
        print 'no record'

    raise gen.Return(data)

@gen.coroutine
def remove_node(unicorn, path):
    print 'checking node {}'.format(path)

    ch = yield unicorn.get(path)
    _, version = yield ch.rx.get()

    if version == -1:
        print 'no node'
        return

    print 'removing node'
    ch = yield unicorn.remove(path, version)
    yield ch.rx.get()


@click.group()
def cli():
    click.echo('processing commnads...')


@cli.command()
@click.option('--path', default=DEFAULT_FILTER_PATH, help='subscription node, default {}'.format(DEFAULT_FILTER_PATH))
def view(path):
    '''View control filter record'''
    unicorn = get_secure_unicorn()
    click.echo('loading record...')
    data = IOLoop.current().run_sync(lambda: get(unicorn, path))
    print 'record: \n{}'.format(data)

@cli.command()
@click.option('--path', default=DEFAULT_FILTER_PATH, help='node to delete, default {}'.format(DEFAULT_FILTER_PATH))
def delete(path):
    '''Delete specified node'''
    unicorn = get_secure_unicorn()
    click.echo('deleting record')
    IOLoop.current().run_sync(lambda: remove_node(unicorn, path))


@cli.command()
@click.option('--path', default=DEFAULT_FILTER_PATH, help='subscription node, default {}'.format(DEFAULT_FILTER_PATH))
@click.option('--apply-control/--no-apply-control', default=False, help='apply_control flag')
@click.option('--white-list', default='', type=click.STRING, help='comma separated prefixes for white list')
def modify(path, apply_control, white_list):
    '''Modify or delete control filter record'''

    unicorn = get_secure_unicorn()
    white_list = [item.strip() for item in white_list.split(',') if item]

    r = dict(
        apply_control=bool(apply_control),
        white_list=white_list
    )
    return IOLoop.current().run_sync(lambda: update(unicorn, path, r))


if __name__ == '__main__':
    cli()
