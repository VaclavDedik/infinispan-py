
import struct


class Encoder(object):
    def __init__(self, byte_array=''):
        self._byte_array = byte_array

    def byte(self, b):
        self._append(struct.pack('>B', b))
        return self

    def uintvar(self, uintvar):
        bits = uintvar & 0x7f
        uintvar >>= 7
        while uintvar:
            self._append(chr(0x80 | bits))
            bits = uintvar & 0x7f
            uintvar >>= 7
        self._append(chr(bits))
        return self

    def string(self, string):
        self._append(string)
        return self

    def len_str(self, string):
        if string:
            self.uintvar(len(string))
            self.string(string)
        else:
            self.byte(0)
        return self

    def encode(self):
        return self._byte_array

    def _append(self, byte_array):
        self._byte_array += byte_array


class Decoder(object):
    def __init__(self, byte_array):
        self._byte_array = byte_array

    def byte(self):
        b = struct.unpack('>B', self._byte_array[0])[0]
        self._shift(1)
        return b

    def uintvar(self):
        b = self.byte()
        uintvar = b & 0x7f
        i = 1
        while b & 0x80:
            b = self.byte()
            uintvar += (b & 0x7f) << 7*i
            i += 1
        return uintvar

    def string(self, n):
        string = self._byte_array[0:n]
        self._shift(n)
        return string

    def len_str(self):
        n = self.uintvar()
        return self.string(n) if n else 0

    def _shift(self, n):
        self._byte_array = self._byte_array[n:]
