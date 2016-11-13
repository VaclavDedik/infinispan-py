# -*- coding: utf-8 -*-

import pytest

from .server import InfinispanServer
from infinispan import connection, hotrod, exception
from infinispan.hotrod import Status


class TestHotrod(object):
    @classmethod
    def setup_class(cls):
        cls.server = InfinispanServer()
        cls.server.start()

    @classmethod
    def teardown_class(cls):
        try:
            cls.server.stop()
        except RuntimeError:
            # is ok, already stopped
            pass

    @pytest.yield_fixture
    def protocol(self):
        conn = connection.SocketConnection()
        protocol = hotrod.Protocol(conn)
        yield protocol
        conn.disconnect()

    def test_server_connection(self, protocol):
        request = hotrod.PingRequest()
        response = protocol.send(request)

        assert response.header.status == Status.OK

    def test_put(self, protocol):
        request = hotrod.PutRequest(key="test", value="ahoj")
        response = protocol.send(request)

        assert response.header.status == Status.OK

    def test_get(self, protocol):
        request = hotrod.GetRequest(key="test")
        response = protocol.send(request)

        assert response.header.status == Status.OK
        assert response.value == "ahoj"

    def test_get_non_existing(self, protocol):
        request = hotrod.GetRequest(key="doesntexist")
        response = protocol.send(request)

        assert response.header.status == Status.KEY_DOES_NOT_EXISTS

    def test_remove(self, protocol):
        request = hotrod.RemoveRequest(key="test")
        response = protocol.send(request)

        assert response.header.status == Status.OK

    def test_contains_key(self, protocol):
        request = hotrod.ContainsKeyRequest(key="test")
        response = protocol.send(request)

        assert response.header.status == Status.KEY_DOES_NOT_EXISTS

    def test_error_unknown_version(self, protocol):
        request = hotrod.GetRequest(
            header=hotrod.RequestHeader(version=19), key="test")
        response = protocol.send(request)

        assert response.header.op == hotrod.ErrorResponse.OP_CODE
        # Should actually be unknown version, there is a bug in infinispan tho
        assert response.header.status == Status.SERVER_ERR

    # This test should be last as it stops the server
    def test_error_server_shutdown(self, protocol):
        TestHotrod.server.stop()
        request = hotrod.GetRequest(key="test")
        with pytest.raises(exception.ConnectionError):
            protocol.send(request)
