from cocaine.burlak.helpers import flatten_dict, flatten_dict_rec

import pytest


TESTS = [
    (
        dict(
            a=1,
            b=2,
            c=dict(z=100, x=500, y=60, d=dict(q=42, z={}, y=dict(a=[]))),
            d=dict(r=dict())
        ),
        {
            'a': 1,
            'b': 2,
            'c.z': 100,
            'c.x': 500,
            'c.y': 60,
            'c.d.q': 42,
            'c.d.z': {},
            'c.d.y.a': [],
            'd.r': {},
        }
    ),
    (
        dict(
            a=1,
            z=[2, 5, 6],
            c=dict(z=100, d=dict(q=42)),
            d=dict(r=dict(k=dict(f=100500), m=dict()))
        ),
        {
            'a': 1,
            'z': [2, 5, 6],
            'c.z': 100,
            'c.d.q': 42,
            'd.r.k.f': 100500,
            'd.r.m': {},
        }
    ),
    (
        dict(), {}
    ),
    (
        [], {}
    )
]


@pytest.mark.parametrize('dictionary, expected', TESTS)
def test_flatten_rec(dictionary, expected):
    assert dict(flatten_dict_rec(dictionary)) == expected


@pytest.mark.parametrize('dictionary, expected', TESTS)
def test_flatten(dictionary, expected):
    assert dict(flatten_dict(dictionary)) == expected
