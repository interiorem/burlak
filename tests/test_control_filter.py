from cocaine.burlak import burlak, config
from cocaine.burlak.context import Context, LoggerSetup
from cocaine.burlak.control_filter import ControlFilter

import pytest

from tornado import queues

from .common import ASYNC_TESTS_TIMEOUT
from .common import make_future, make_logger_mock, make_mock_channel_with


CF = ControlFilter


test_filters = [
    # control_filter, unicorn version
    (dict(apply_control=True, white_list=[]), 0),
    (dict(apply_control=False, white_list=[]), 1),
    (dict(apply_control=True, white_list=['a', 'b', 'c']), 2),
    (dict(apply_control=False, white_list=['c', 'b', 'a']), 3),
]


except_filters = [
    Exception('error', "I'm broken 1"),
    (ControlFilter.with_defaults(), 1),
    Exception('error', "I'm broken 2"),
    Exception('error', "I'm broken 2"),
]


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


@pytest.fixture
def filter_listener(mocker):
    logger = make_logger_mock(mocker)
    filter_queue, input_queue = queues.Queue(), queues.Queue()

    cfg = config.Config(mocker.Mock())
    sentry_wrapper = mocker.Mock()

    context = Context(
        LoggerSetup(logger, False),
        cfg,
        '0',
        sentry_wrapper,
        mocker.Mock(),
    )

    unicorn = mocker.Mock()
    unicorn.subscribe = mocker.Mock()

    return burlak.ControlFilterListener(
        context, unicorn, filter_queue, input_queue)


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_filter_listener(filter_listener, mocker):
    stop_side_effect = [True for _ in test_filters]
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)
    mocker.patch('tornado.gen.sleep', return_value=make_future(0))

    filter_listener.unicorn.subscribe = mocker.Mock(
        side_effect=[make_mock_channel_with(*test_filters)]
    )

    yield filter_listener.subscribe_to_control_filter()

    if test_filters:
        first_update = yield filter_listener.filter_queue.get()
        filter_listener.filter_queue.task_done()

        f, _ = test_filters[0]

        assert first_update.control_filter == ControlFilter.from_dict(f)


@pytest.mark.gen_test(timeout=ASYNC_TESTS_TIMEOUT)
def test_filter_with_except(filter_listener, mocker):
    stop_side_effect = [True for _ in except_filters]
    stop_side_effect.append(False)

    mocker.patch.object(
        burlak.LoopSentry, 'should_run', side_effect=stop_side_effect)
    mocker.patch('tornado.gen.sleep', return_value=make_future(0))

    filter_listener.unicorn.subscribe = mocker.Mock(
        side_effect=[make_mock_channel_with(*except_filters)]
    )

    yield filter_listener.subscribe_to_control_filter()

    if except_filters:

        cfg = config.Config(mocker.Mock())

        first_update = yield filter_listener.filter_queue.get()
        filter_listener.filter_queue.task_done()

        assert first_update.control_filter == cfg.control_filter
