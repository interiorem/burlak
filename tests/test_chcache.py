from cocaine.burlak import ChannelsCache
from cocaine.burlak.chcache import _AppsCache

from cocaine.services import Service

import pytest

from .common import ASYNC_TESTS_TIMEOUT, make_mock_channel_with


TEST_CONTROL_VALUE = 42


apps_list = ['app{}'.format(i) for i in xrange(4)]


@pytest.fixture
def ch_cache(mocker):
    logger = mocker.Mock()

    mocker.patch.object(
        _AppsCache,
        'make_control_ch',
        return_value=make_mock_channel_with(0))

    return ChannelsCache(logger)


@pytest.fixture
def app_cache(mocker):
    Service.control = mocker.Mock()

    mocker.patch.object(
        Service, 'control',
        return_value=make_mock_channel_with(TEST_CONTROL_VALUE))

    logger = mocker.Mock()
    return _AppsCache(logger)


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_close_all(ch_cache):
    yield [ch_cache.get_ch(app) for app in apps_list]

    assert len(ch_cache.channels.keys()) == len(apps_list)

    yield ch_cache.close_and_remove_all()

    assert not ch_cache.channels


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_close_all_with_flag(ch_cache):
    yield [ch_cache.get_ch(app) for app in apps_list]
    yield [ch_cache.add_one(app, should_close=True) for app in apps_list]

    assert len(ch_cache.channels.keys()) == len(apps_list)

    yield ch_cache.close_and_remove_all()

    assert not ch_cache.channels


@pytest.mark.parametrize(
    'app',
    [
        ('zoo'),
        ('boo')
    ]
)
@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_app_cache(app_cache, app):
    ch = yield app_cache.make_control_ch(app)
    result = yield ch.rx.get()

    assert result == TEST_CONTROL_VALUE
    assert len(app_cache) == 1

    app_cache.remove([app])
    assert len(app_cache) == 0
