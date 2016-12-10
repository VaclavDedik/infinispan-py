# -*- coding: utf-8 -*-

import struct

from builtins import bytes

import infinispan as ispn

from infinispan import error


class Encoder(object):
    def __init__(self, byte_array=bytes()):
        self._byte_array = byte_array

    def encode(self, message):
        """Encodes a message (request or a response).

        :param message: Message you want to encode.
        :return: Byte array which represents the encoded message.
        """

        return self._encode(message).result()

    def _encode(self, message):
        for f_name in message.fields:
            f = getattr(message, f_name)
            f_cls = getattr(message.__class__, f_name)

            # test if field is available only under condition
            if hasattr(f_cls, 'condition') and not f_cls.condition(message):
                continue
            # test if field is none and raise an error if so (unless optional)
            if f is None and \
                    not (hasattr(f_cls, 'optional') and f_cls.optional):
                raise error.EncodeError(
                    "Field '%s' of '%s#%s' must not be None",
                    f, type(message).__name__, f_name)

            if f_cls.type == "composite":
                self._encode(f)
            elif f_cls.type == "list":
                for elem in f:
                    self._encode(elem)
            else:
                getattr(self, f_cls.type)(f)
        return self

    def byte(self, b):
        self._append(struct.pack('>B', b))
        return self

    def bytes(self, byte_array):
        self.uvarint(len(byte_array))
        self._append(byte_array)
        return self

    def splitbyte(self, b2):
        self.byte((b2[0] << 4) + b2[1])
        return self

    def ushort(self, ushort):
        self._append(struct.pack('>H', ushort))
        return self

    def uvarint(self, uvarint):
        encoded_uvar = self._uvar(uvarint)
        if len(encoded_uvar) > 5:
            raise error.EncodeError("Value too high")
        self._append(encoded_uvar)
        return self

    def uvarlong(self, uvarlong):
        encoded_uvar = self._uvar(uvarlong)
        if len(encoded_uvar) > 9:
            raise error.EncodeError("Value too high")
        self._append(encoded_uvar)
        return self

    def string(self, string):
        if string:
            self.bytes(string.encode("UTF-8"))
        else:
            self.byte(0x00)
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
    _OPS = None

    def __init__(self, byte_gen=None):
        self._byte_gen = byte_gen

    def decode(self, data):
        """Decodes a response from a byte array.

        :param data: Byte array that represents the response.
        :return: Response object.
        """

        rh = ispn.hotrod.ResponseHeader()
        self._byte_gen = data
        self._decode(rh)

        # if ops map not yet initialized, init it (can't be done statically)
        if not self._OPS:
            self._OPS = {r.OP_CODE: r for r in ispn.utils.get_all_subclasses(
                         ispn.hotrod.Response) if hasattr(r, 'OP_CODE')}
        try:
            response = self._OPS[rh.op](header=rh)
        except KeyError:
            raise error.DecodeError(
                "Response operation with code %s is not supported.", rh.op)
        self._decode(response, skip_fields=1)

        return response

    def _decode(self, message, skip_fields=0):
        for f_name in message.fields[skip_fields:]:
            f = getattr(message, f_name)
            f_cls = getattr(message.__class__, f_name)

            if 'condition' in dir(f_cls) and not f_cls.condition(message):
                continue

            if f_cls.type == "composite":
                self._decode(f)
            elif f_cls.type == "list":
                l = []
                for _ in range(f_cls.size(message)):
                    elem = f_cls.of()
                    l.append(elem)
                    self._decode(elem)
                setattr(message, f_name, l)
            else:
                decoded = getattr(self, f_cls.type)()
                setattr(message, f_name, decoded)

    def byte(self):
        b = ord(self._read_next())
        return b

    def bytes(self):
        n = self.uvarint()
        byte_array = bytes()
        for i in range(n):
            byte_array += bytes([self.byte()])
        return byte_array

    def ushort(self):
        return (self.byte() << 8) + self.byte()

    def splitbyte(self):
        b = self.byte()
        b2 = [b >> 4, b & 0x0f]
        return b2

    def uvarint(self):
        return self._uvar(maxlen=5)

    def uvarlong(self):
        return self._uvar(maxlen=9)

    def string(self):
        return self.bytes().decode('UTF-8')

    def _uvar(self, maxlen=5):
        b = self.byte()
        uvar = b & 0x7f
        i = 1
        while b & 0x80:
            if i + 1 > maxlen:
                raise error.DecodeError("Value too high")
            b = self.byte()
            uvar += (b & 0x7f) << 7*i
            i += 1
        return uvar

    def _read_next(self):
        try:
            byte = next(self._byte_gen)
        except StopIteration:
            raise error.DecodeError(
                "Unexpected end of byte array generator")
        if not byte:
            raise error.DecodeError("Value is empty")
        return byte


class EncoderFactory(object):
    def get(self):
        return Encoder()


class DecoderFactory(object):
    def get(self):
        return Decoder()
