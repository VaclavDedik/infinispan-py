# -*- coding: utf-8 -*-

import pytest

from mock import MagicMock
from infinispan import hotrod, exception


class TestProtocol(object):
    @pytest.fixture
    def protocol(self):
        protocol = hotrod.Protocol(MagicMock())
        return protocol

    def test_encode(self, protocol):
        rh = hotrod.RequestHeader(id=3, op=0x01)
        expected = b'\xa0\x03\x19\x01\x00\x00\x01\x00'
        actual = protocol.encode(rh)

        assert expected == actual

    def test_encode_fail_all_values_not_set(self, protocol):
        with pytest.raises(exception.EncodeError):
            rh = hotrod.RequestHeader()
            protocol.encode(rh)

    def test_decode(self, protocol):
        data = iter('\xa1\x03\x04\x00\x00\x04ahoj')
        expected = hotrod.GetResponse(
            header=hotrod.ResponseHeader(id=3), value="ahoj")
        actual = protocol.decode(data)

        assert expected.header.magic == actual.header.magic
        assert expected.header.id == actual.header.id
        assert expected.header.op == actual.header.op
        assert expected.header.status == actual.header.status
        assert expected.header.tcm == actual.header.tcm
        assert expected.value == actual.value
