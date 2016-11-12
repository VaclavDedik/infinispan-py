
import struct

from infinispan import exception


class Encoder(object):
    def __init__(self, byte_array=bytes()):
        self._byte_array = byte_array

    def byte(self, b):
        self._append(struct.pack('>B', b))
        return self

    def splitbyte(self, b2):
        self.byte((b2[0] << 4) + b2[1])
        return self

    def uvarint(self, uvarint):
        encoded_uvar = self._uvar(uvarint)
        if len(encoded_uvar) > 5:
            raise exception.EncodeError("Value too high")
        self._append(encoded_uvar)
        return self

    def uvarlong(self, uvarlong):
        encoded_uvar = self._uvar(uvarlong)
        if len(encoded_uvar) > 9:
            raise exception.EncodeError("Value too high")
        self._append(encoded_uvar)
        return self

    def string(self, string):
        for c in string:
            self.byte(ord(c))
        return self

    def lenstr(self, string):
        if string:
            self.uvarint(len(string))
            self.string(string)
        else:
            self.byte(0)
        return self

    def result(self):
        return self._byte_array

    def _uvar(self, uvar):
        result = bytes()
        bits = uvar & 0x7f
        uvar >>= 7
        while uvar:
            result += struct.pack('>B', 0x80 | bits)
            bits = uvar & 0x7f
            uvar >>= 7
        result += struct.pack('>B', bits)
        return result

    def _append(self, byte_array):
        self._byte_array += byte_array


class Decoder(object):
    def __init__(self, byte_gen):
        self._byte_gen = byte_gen

    def byte(self):
        b = ord(self._read_next())
        return b

    def splitbyte(self):
        b = self.byte()
        b2 = [b >> 4, b & 0x0f]
        return b2

    def uvarint(self):
        return self._uvar(maxlen=5)

    def uvarlong(self):
        return self._uvar(maxlen=9)

    def string(self, n):
        string = ''
        for i in range(n):
            string += chr(self.byte())
        return string

    def lenstr(self):
        n = self.uvarint()
        return self.string(n) if n else ''

    def _uvar(self, maxlen=5):
        b = self.byte()
        uvar = b & 0x7f
        i = 1
        while b & 0x80:
            if i + 1 > maxlen:
                raise exception.DecodeError("Value too high")
            b = self.byte()
            uvar += (b & 0x7f) << 7*i
            i += 1
        return uvar

    def _read_next(self):
        try:
            byte = next(self._byte_gen)
        except StopIteration:
            raise exception.DecodeError(
                "Unexpected end of byte array generator")
        if not byte:
            raise exception.DecodeError("Value is empty")
        return byte
