from cocaine.burlak.burlak import filter_apps

import pytest


apps = [
    'zoo',
    'zooloo',
    'zork',
    'zd',
    'boo',
    'bo',
    'boomerang',
    'boorka boom',
]

apps_set = set(apps)
apps_list = apps
apps_dict = dict(map(lambda x: (x, None), apps))


def test_set_void_filter():
    subset = filter_apps(apps_set, [])
    assert subset == apps_set


@pytest.mark.parametrize(
    'prefix,count',
    [
        ('z', 4),
        ('zo', 3),
        ('zoo', 2),
        ('zd', 1),

        ('b', 4),
        ('bo', 4),
        ('boo', 3),
        ('boom', 1),
        ('boorka ', 1),
    ]
)
def test_set_filter(prefix, count):
    subset = filter_apps(set(apps), [prefix])
    assert len(subset) == count


def test_dict_void_filter():
    subset = filter_apps(apps_dict, [])
    assert subset == apps_dict


def test_none_filter():
    subset = filter_apps(None, ['a', 'b', 'c'])
    assert subset is None


@pytest.mark.parametrize(
    'prefix,count',
    [
        ('z', 4),
        ('zo', 3),
        ('zoo', 2),
        ('zd', 1),

        ('b', 4),
        ('bo', 4),
        ('boo', 3),
        ('boom', 1),
        ('boorka ', 1),
    ]
)
def test_dict_filter(prefix, count):
    subset = filter_apps(apps_dict, [prefix])
    assert len(subset) == count


@pytest.mark.parametrize(
    'prefix,count',
    [
        ('z', 4),
        ('zo', 3),
        ('zoo', 2),
        ('zd', 1),

        ('b', 4),
        ('bo', 4),
        ('boo', 3),
        ('boom', 1),
        ('boorka ', 1),
    ]
)
def test_list_filter(prefix, count):
    subset = filter_apps(apps_list, [prefix])
    assert len(subset) == count


@pytest.mark.parametrize(
    'prefixes,count',
    [
        (['zo', 'z'], 4),
        (['zo', 'zoo'], 3),
        (['zo', 'boo'], 6),
        ([], len(apps)),
    ]
)
def test_multiple_filter(prefixes, count):
    subset = filter_apps(apps_dict, prefixes)
    assert len(subset) == count

    subset = filter_apps(apps_set, prefixes)
    assert len(subset) == count
