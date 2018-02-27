from cocaine.burlak.control_filter import ControlFilter

import pytest

CF = ControlFilter


def eq(a, b):
    return a == b


def ne(a, b):
    return a != b


@pytest.mark.parametrize(
    'a,b,op',
    [
        (CF(True, ['a', 'b', 'c']), CF(True, ['a', 'b', 'c']), eq),
        (CF(True, ['a', 'b', 'c']), CF(True, ['a', 'b', 'z']), ne),
        (CF(True, ['a', 'b', 'c']), CF(False, ['a', 'b', 'c']), ne),
        (CF(True, ['a', 'b', 'c', 'k']), CF(True, ['a', 'b', 'c']), ne),
    ]
)
def test_filter_eq(a, b, op):
    assert op(a, b)
