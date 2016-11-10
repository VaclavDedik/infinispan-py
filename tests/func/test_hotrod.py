
import pytest
import time

from server import InfinispanServer
from infinispan import connection, hotrod
from infinispan.hotrod import Status


class TestHotrod(object):
    @classmethod
    def setup_class(cls):
        cls.server = InfinispanServer()
        cls.server.start()
        time.sleep(5)

    @classmethod
    def teardown_class(cls):
        cls.server.stop()

    @pytest.fixture
    def protocol(self):
        conn = connection.SocketConnection()
        protocol = hotrod.Protocol(conn)
        return protocol

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
