# -*- coding: utf-8 -*-

import jsonpickle

from past.builtins import basestring

from infinispan import exception


class Serialization(object):
    def serialize(self, obj):
        raise NotImplementedError

    def deserialize(self, byte_array):
        raise NotImplementedError


class UTF8(Serialization):
    def serialize(self, string):
        if not string:
            return None
        elif not isinstance(string, basestring):
            raise exception.SerializationError("Value must be a string.")
        else:
            return string.encode("UTF-8")

    def deserialize(self, byte_array):
        if not byte_array:
            return None
        else:
            return byte_array.decode("UTF-8")


class JSONPickle(Serialization):
    def serialize(self, obj):
        return jsonpickle.encode(obj).encode("UTF-8")

    def deserialize(self, byte_array):
        if byte_array:
            return jsonpickle.decode(byte_array.decode("UTF-8"))
        else:
            return None
