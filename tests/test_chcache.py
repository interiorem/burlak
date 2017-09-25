from cocaine.burlak import ChannelsCache

import pytest

from .common import ASYNC_TESTS_TIMEOUT, make_mock_channel_with


apps_list = ['app{}'.format(i) for i in xrange(4)]


@pytest.fixture
def ch_cache(mocker):
    logger = mocker.Mock()
    node = mocker.Mock()
    node.control = mocker.Mock(return_value=make_mock_channel_with(0))

    return ChannelsCache(logger, node)


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
