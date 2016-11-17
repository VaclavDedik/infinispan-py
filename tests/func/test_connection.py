# -*- coding: utf-8 -*-

import pytest

from .server import InfinispanServer
from infinispan import connection, exception


class TestSocketConnection(object):
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
    def conn(self):
        conn = connection.SocketConnection()
        conn.connect()
        yield conn
        conn.disconnect()

    def test_successful_connection(self, conn):
        conn.send(b'\xa0\x01\x19\x17\x00\x00\x01\x00')

        assert next(conn.recv()) == b'\xa1'

    def test_remote_hung_up(self, conn):
        TestSocketConnection.server.stop()
        try:
            with pytest.raises(exception.ConnectionError):
                conn.send(b'\xa0\x01\x19\x17\x00\x00\x01\x00')
                next(conn.recv())
        finally:
            TestSocketConnection.server.start()

    def test_remote_conn_refused(self, conn):
        TestSocketConnection.server.kill()
        try:
            with pytest.raises(exception.ConnectionError):
                conn.send(b'\xa0\x01\x19\x17\x00\x00\x01\x00')
                next(conn.recv())
        finally:
            TestSocketConnection.server.start()

    def test_disconnect_before_send(self, conn):
        TestSocketConnection.server.stop()
        try:
            with pytest.raises(exception.ConnectionError):
                conn.send(b'\xa0\x01\x19\x17\x00\x00\x01\x00')
                conn.send(b'\xa0\x02\x19\x17\x00\x00\x01\x00')
        finally:
            TestSocketConnection.server.start()

    def test_connection_timeout(self, conn):
        with pytest.raises(exception.ConnectionError):
            next(conn.recv())
