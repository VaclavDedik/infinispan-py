# -*- coding: utf-8 -*-

import time
import pytest

from mock import MagicMock

from infinispan.client import Infinispan
from infinispan import connection


class TestClientWithMockConnection(object):
    @pytest.yield_fixture
    def client(self):
        client = Infinispan()
        client.protocol.conn = connection.ConnectionPool(
            [self._new_conn() for _ in range(20)])
        yield client
        client.disconnect()

    def test_ping_async_1000x_with_connection_pool(self, client, benchmark):
        client.connect()

        results = benchmark(self._ping_async_1000x, client)
        assert all(results)

    def test_ping_sync_1000x_with_connection_pool(self, client, benchmark):
        client.connect()

        results = benchmark(self._ping_sync_1000x, client)
        assert all(results)

    def test_ping_async_1000x_with_socket_connection(self, client, benchmark):
        client.protocol.conn = self._new_conn()
        client.connect()

        results = benchmark(self._ping_async_1000x, client)
        assert all(results)

    def _ping_async_1000x(self, client):
        futures = []
        for _ in range(1000):
            futures.append(client.ping_async())
        return map(lambda f: f.result(), futures)

    def _ping_sync_1000x(self, client):
        results = []
        for _ in range(1000):
            results.append(client.ping())
        return results

    def _new_conn(self):
        conn = connection.SocketConnection()

        def connect():
            conn._s = MagicMock()
        conn.connect = connect

        def send(byte_array):
            time.sleep(0.001)
        conn.send = send

        def recv():
            time.sleep(0.002)
            result = iter(b'\xa1\x01\x18\x00\x00')
            while True:
                time.sleep(0.0001)
                yield next(result)
        conn.recv = recv

        return conn
