# -*- coding: utf-8 -*-

import pytest

from infinispan import codec, exception


class TestEncoder(object):

    @pytest.fixture
    def encoder(self):
        return codec.Encoder()

    def test_encode_byte(self, encoder):
        byte = 0x33
        expected = b'\x33'
        actual = encoder.byte(byte).result()

        assert expected == actual

    def test_encode_splitbyte(self, encoder):
        byte2 = [0x07, 0x06]
        expected = b'\x76'
        actual = encoder.splitbyte(byte2).result()

        assert expected == actual

    def test_encode_uvarint(self, encoder):
        uvarint = 1000
        expected = b'\xe8\x07'
        actual = encoder.uvarint(uvarint).result()

        assert expected == actual

    def test_encode_uvarint_fail_too_long(self, encoder):
        with pytest.raises(exception.EncodeError):
            uvarint = 2**(7*5)
            encoder.uvarint(uvarint).result()

    def test_encode_uvarlong(self, encoder):
        uvarlong = 2**32
        expected = b'\x80\x80\x80\x80\x10'
        actual = encoder.uvarlong(uvarlong).result()

        assert expected == actual

    def test_encode_uvarlong_fail_too_long(self, encoder):
        with pytest.raises(exception.EncodeError):
            uvarlong = 2**(7*9)
            encoder.uvarlong(uvarlong).result()

    def test_encode_lenstr(self, encoder):
        string = 'ahoj'
        expected = b'\x04ahoj'
        actual = encoder.lenstr(string).result()

        assert expected == actual

    def test_encode_utf8_lenstr(self, encoder):
        string = u'řahoj'
        expected = b'\x06\xc5\x99ahoj'
        actual = encoder.lenstr(string).result()

        assert expected == actual


class TestDecoder(object):

    def test_decode_byte(self):
        byte = iter('\x33')
        expected = 0x33
        actual = codec.Decoder(byte).byte()

        assert expected == actual

    def test_decode_splitbyte(self):
        byte2 = iter('\x76')
        expected = [0x07, 0x06]
        actual = codec.Decoder(byte2).splitbyte()

        assert expected == actual

    def test_decode_uvarint(self):
        uvarint = iter('\xe8\x07')
        expected = 1000
        actual = codec.Decoder(uvarint).uvarint()

        assert expected == actual

    def test_decode_uvarint_fail_too_long(self):
        with pytest.raises(exception.DecodeError):
            uvarint = iter('\x80\x80\x80\x80\x80\x01')
            codec.Decoder(uvarint).uvarint()

    def test_decode_uvarlong(self):
        uvarlong = iter('\x80\x80\x80\x80\x10')
        expected = 2**32
        actual = codec.Decoder(uvarlong).uvarlong()

        assert expected == actual

    def test_decode_uvarlong_fail_too_long(self):
        with pytest.raises(exception.DecodeError):
            uvarlong = iter('\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01')
            codec.Decoder(uvarlong).uvarlong()

    def test_decode_lenstr(self):
        string = iter('\x04ahoj')
        expected = 'ahoj'
        actual = codec.Decoder(string).lenstr()

        assert expected == actual

    def test_decode_utf8_lenstr(self):
        string = iter('\x06\xc5\x99ahoj')
        expected = u'řahoj'
        actual = codec.Decoder(string).lenstr()

        assert expected == actual

    def test_decode_empty_byte(self):
        with pytest.raises(exception.DecodeError):
            codec.Decoder(iter([''])).byte()
