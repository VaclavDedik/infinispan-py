# -*- coding: utf-8 -*-

from past.builtins import basestring

from infinispan import exception


class Serialization(object):
    def serialize(self, obj):
        pass

    def deserialize(self, byte_array):
        pass


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
