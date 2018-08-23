from cocaine.burlak.uniresis import catchup_an_uniresis
from cocaine.services import Service

import pytest

from .common import MockChannel, make_future


TESTING_UUID = 'TEST_UUID'


@pytest.fixture
def context(mocker):
    context = mocker.Mock()
    context.config = mocker.Mock()
    type(context.config).api_timeout = mocker.PropertyMock(return_value=100500)
    return context


@pytest.mark.gen_test
def test_uniresis(context):
    uuid = yield catchup_an_uniresis(
        context, use_stub_uuid=TESTING_UUID).uuid()
    assert uuid == TESTING_UUID


@pytest.mark.gen_test
def test_uniresis_stub(mocker, context):
    mocker.patch.object(
        Service, 'uuid', create=True,
        return_value=make_future(MockChannel(TESTING_UUID)))

    uniresis = catchup_an_uniresis(context)

    uuid = yield uniresis.uuid()
    assert uuid == TESTING_UUID
