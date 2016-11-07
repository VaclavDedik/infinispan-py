
import struct


class Encoder(object):
    def __init__(self, byte_array=''):
        self._byte_array = byte_array

    def byte(self, b):
        self._append(struct.pack('>B', b))
        return self

    def splitbyte(self, b2):
        self.byte((b2[0] << 4) + b2[1])
        return self

    def uvarint(self, uvarint):
        bits = uvarint & 0x7f
        uvarint >>= 7
        while uvarint:
            self._append(chr(0x80 | bits))
            bits = uvarint & 0x7f
            uvarint >>= 7
        self._append(chr(bits))
        return self

    def uvarlong(self, uvarlong):
        self.uvarint(uvarlong)
        return self

    def string(self, string):
        self._append(string)
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

    def _append(self, byte_array):
        self._byte_array += byte_array


class Decoder(object):
    def __init__(self, byte_gen):
        self._byte_gen = byte_gen

    def byte(self):
        b = struct.unpack('>B', self._byte_gen.next())[0]
        return b

    def splitbyte(self):
        b = self.byte()
        b2 = [b >> 4, b & 0x0f]
        return b2

    def uvarint(self):
        b = self.byte()
        uvarint = b & 0x7f
        i = 1
        while b & 0x80:
            b = self.byte()
            uvarint += (b & 0x7f) << 7*i
            i += 1
        return uvarint

    def uvarlong(self):
        return self.uvarint()

    def string(self, n):
        string = ''
        for i in range(n):
            string += self._byte_gen.next()
        return string

    def lenstr(self):
        n = self.uvarint()
        return self.string(n) if n else 0
