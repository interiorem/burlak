from cocaine.burlak.uniresis import catchup_an_uniresis
from cocaine.services import Service

import pytest

from .common import MockChannel, make_future


TESTING_UUID = 'TEST_UUID'


@pytest.mark.gen_test
def test_uniresis():
    from cocaine.burlak.uniresis import DummyProxy

    uuid = yield catchup_an_uniresis(use_stub=True).uuid()
    assert uuid == DummyProxy.COCAINE_TEST_UUID


@pytest.mark.gen_test
def test_uniresis_stub(mocker):
    mocker.patch.object(
        Service, 'uuid', create=True,
        return_value=make_future(MockChannel(TESTING_UUID)))

    uniresis = catchup_an_uniresis(use_stub=False)

    uuid = yield uniresis.uuid()
    assert uuid == TESTING_UUID
