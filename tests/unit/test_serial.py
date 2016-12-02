# -*- coding: utf-8 -*-

import pytest

from infinispan import error
from infinispan.serial import UTF8, JSONPickle


class TestUTF8(object):
    def test_serialize_string(self):
        assert UTF8().serialize("ahoj") == b'ahoj'

    def test_serialize_non_string(self):
        with pytest.raises(error.SerializationError):
            UTF8().serialize(1)

    def test_deserialize(self):
        assert UTF8().deserialize(b'ahoj') == "ahoj"


class TestJSONPickle(object):
    def test_serialize_string(self):
        assert JSONPickle().serialize("ahoj") == b'"ahoj"'

    def test_serialize_int(self):
        assert JSONPickle().serialize(1) == b'1'

    def test_deserialize_string(self):
        assert JSONPickle().deserialize(b'"ahoj"') == "ahoj"

    def test_deserialize_int(self):
        assert JSONPickle().deserialize(b'1') == 1
