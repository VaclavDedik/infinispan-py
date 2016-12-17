# -*- coding: utf-8 -*-

import pytest

from infinispan import codec, error, hotrod


class TestEncoder(object):

    @pytest.fixture
    def encoder(self):
        return codec.Encoder()

    def test_encode_byte(self, encoder):
        byte = 0x33
        expected = b'\x33'
        actual = encoder.byte(byte).result()

        assert expected == actual

    def test_encode_bytes(self, encoder):
        bytes_ = b'ahoj\x76'
        expected = b'ahoj\x76'
        actual = encoder.bytes(bytes_, 5).result()

        assert expected == actual

    def test_encode_varbytes(self, encoder):
        bytes_ = b'ahoj\x76'
        expected = b'\x05ahoj\x76'
        actual = encoder.varbytes(bytes_).result()

        assert expected == actual

    def test_encode_splitbyte(self, encoder):
        byte2 = [0x07, 0x06]
        expected = b'\x76'
        actual = encoder.splitbyte(byte2).result()

        assert expected == actual

    def test_encode_ushort(self, encoder):
        short = 11211
        expected = b'\x2b\xcb'
        actual = encoder.ushort(short).result()

        assert expected == actual

    def test_encode_uvarint(self, encoder):
        uvarint = 1000
        expected = b'\xe8\x07'
        actual = encoder.uvarint(uvarint).result()

        assert expected == actual

    def test_encode_uvarint_fail_too_long(self, encoder):
        with pytest.raises(error.EncodeError):
            uvarint = 2**(7*5)
            encoder.uvarint(uvarint).result()

    def test_encode_uvarlong(self, encoder):
        uvarlong = 2**32
        expected = b'\x80\x80\x80\x80\x10'
        actual = encoder.uvarlong(uvarlong).result()

        assert expected == actual

    def test_encode_uvarlong_fail_too_long(self, encoder):
        with pytest.raises(error.EncodeError):
            uvarlong = 2**(7*9)
            encoder.uvarlong(uvarlong).result()

    def test_encode_string(self, encoder):
        string = 'ahoj'
        expected = b'\x04ahoj'
        actual = encoder.string(string).result()

        assert expected == actual

    def test_encode_utf8_string(self, encoder):
        string = u'řahoj'
        expected = b'\x06\xc5\x99ahoj'
        actual = encoder.string(string).result()

        assert expected == actual

    def test_encode_long(self, encoder):
        l = 1125899906842624
        expected = b'\x00\x04\x00\x00\x00\x00\x00\x00'
        actual = encoder.long(l).result()

        assert expected == actual

    def test_encode(self, encoder):
        rh = hotrod.RequestHeader(id=3, op=0x01)
        expected = b'\xa0\x03\x19\x01\x00\x00\x01\x00'
        actual = encoder.encode(rh)

        assert expected == actual

    def test_encode_with_list(self, encoder):
        expected = b'\xa1\x03\x04\x00\x01\x03\x02' + \
            b'\t127.0.0.1,l\t127.0.0.1+\xd6\x04ahoj'
        response = hotrod.GetResponse(
            header=hotrod.ResponseHeader(
                id=3, tcm=1, tc=hotrod.TopologyChangeHeader(
                    id=3, n=2, hosts=[
                        hotrod.Host(ip='127.0.0.1', port=11372),
                        hotrod.Host(ip='127.0.0.1', port=11222)
                    ])),
            value=b'ahoj')
        actual = encoder.encode(response)

        assert expected == actual

    def test_encode_fail_all_values_not_set(self, encoder):
        with pytest.raises(error.EncodeError):
            rh = hotrod.RequestHeader()
            encoder.encode(rh)


class TestDecoder(object):

    def test_decode_byte(self):
        byte = iter('\x33')
        expected = 0x33
        actual = codec.Decoder(byte).byte()

        assert expected == actual

    def test_decode_bytes(self):
        bytes_ = iter('ahoj\x76')
        expected = b'ahoj\x76'
        actual = codec.Decoder(bytes_).bytes(5)

        assert expected == actual

    def test_decode_varbytes(self):
        bytes_ = iter('\x05ahoj\x76')
        expected = b'ahoj\x76'
        actual = codec.Decoder(bytes_).varbytes()

        assert expected == actual

    def test_decode_splitbyte(self):
        byte2 = iter('\x76')
        expected = [0x07, 0x06]
        actual = codec.Decoder(byte2).splitbyte()

        assert expected == actual

    def test_decode_ushort(self):
        short = iter('\x2b\xcb')
        expected = 11211
        actual = codec.Decoder(short).ushort()

        assert expected == actual

    def test_decode_uvarint(self):
        uvarint = iter('\xe8\x07')
        expected = 1000
        actual = codec.Decoder(uvarint).uvarint()

        assert expected == actual

    def test_decode_uvarint_fail_too_long(self):
        with pytest.raises(error.DecodeError):
            uvarint = iter('\x80\x80\x80\x80\x80\x01')
            codec.Decoder(uvarint).uvarint()

    def test_decode_uvarlong(self):
        uvarlong = iter('\x80\x80\x80\x80\x10')
        expected = 2**32
        actual = codec.Decoder(uvarlong).uvarlong()

        assert expected == actual

    def test_decode_uvarlong_fail_too_long(self):
        with pytest.raises(error.DecodeError):
            uvarlong = iter('\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01')
            codec.Decoder(uvarlong).uvarlong()

    def test_decode_string(self):
        string = iter('\x04ahoj')
        expected = 'ahoj'
        actual = codec.Decoder(string).string()

        assert expected == actual

    def test_decode_utf8_string(self):
        string = iter('\x06\xc5\x99ahoj')
        expected = u'řahoj'
        actual = codec.Decoder(string).string()

        assert expected == actual

    def test_decode_long(self):
        l = iter('\x00\x04\x00\x00\x00\x00\x00\x00')
        expected = 1125899906842624
        actual = codec.Decoder(l).long()

        assert expected == actual

    def test_decode_empty_byte(self):
        with pytest.raises(error.DecodeError):
            codec.Decoder(iter([''])).byte()

    def test_decode(self):
        data = iter('\xa1\x03\x04\x00\x00\x04ahoj')
        expected = hotrod.GetResponse(
            header=hotrod.ResponseHeader(id=3), value=b'ahoj')
        actual = codec.Decoder().decode(data)

        assert expected.header.magic == actual.header.magic
        assert expected.header.id == actual.header.id
        assert expected.header.op == actual.header.op
        assert expected.header.status == actual.header.status
        assert expected.header.tcm == actual.header.tcm
        assert expected.value == actual.value

    def test_decode_with_list(self):
        data = iter(
            '\xa1\x03\x04\x00\x01\x03\x02' +
            '\t127.0.0.1,l\t127.0.0.1+\xd6\x04ahoj'
        )
        expected = hotrod.GetResponse(
            header=hotrod.ResponseHeader(
                id=3, tcm=1, tc=hotrod.TopologyChangeHeader(
                    id=3, n=2, hosts=[
                        hotrod.Host(ip='127.0.0.1', port=11372),
                        hotrod.Host(ip='127.0.0.1', port=11222)
                    ])),
            value=b'ahoj')
        actual = codec.Decoder().decode(data)

        assert expected.header.magic == actual.header.magic
        assert expected.header.id == actual.header.id
        assert expected.header.op == actual.header.op
        assert expected.header.status == actual.header.status
        assert expected.header.tcm == actual.header.tcm
        assert expected.value == actual.value

        assert expected.header.tc.id == actual.header.tc.id
        assert expected.header.tc.n == actual.header.tc.n
        assert expected.header.tc.hosts[0].ip == actual.header.tc.hosts[0].ip
        assert expected.header.tc.hosts[0].port \
            == actual.header.tc.hosts[0].port
        assert expected.header.tc.hosts[1].ip == actual.header.tc.hosts[1].ip
        assert expected.header.tc.hosts[1].port \
            == actual.header.tc.hosts[1].port
