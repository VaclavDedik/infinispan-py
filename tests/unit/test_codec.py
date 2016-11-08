
import pytest

from infinispan import codec


class TestEncoder(object):

    @pytest.fixture
    def encoder(self):
        return codec.Encoder()

    def test_encode_byte(self, encoder):
        byte = 0x33
        expected = '\x33'
        actual = encoder.byte(byte).result()

        assert expected == actual

    def test_encode_splitbyte(self, encoder):
        byte2 = [0x07, 0x06]
        expected = '\x76'
        actual = encoder.splitbyte(byte2).result()

        assert expected == actual

    def test_encode_uvarint(self, encoder):
        uvarint = 1000
        expected = '\xe8\x07'
        actual = encoder.uvarint(uvarint).result()

        assert expected == actual

    def test_encode_uvarint_fail_too_long(self, encoder):
        with pytest.raises(Exception):
            uvarint = 2**(7*5)
            encoder.uvarint(uvarint).result()

    def test_encode_uvarlong(self, encoder):
        uvarlong = 2**32
        expected = '\x80\x80\x80\x80\x10'
        actual = encoder.uvarlong(uvarlong).result()

        assert expected == actual

    def test_encode_uvarlong_fail_too_long(self, encoder):
        with pytest.raises(Exception):
            uvarlong = 2**(7*9)
            encoder.uvarlong(uvarlong).result()

    def test_encode_string(self, encoder):
        string = "ahoj"
        expected = string
        actual = encoder.string(string).result()

        assert expected == actual

    def test_encode_lenstr(self, encoder):
        string = "ahoj"
        expected = '\x04' + string
        actual = encoder.lenstr(string).result()

        assert expected == actual
