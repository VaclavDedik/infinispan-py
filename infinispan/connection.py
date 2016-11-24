# -*- coding: utf-8 -*-

import time
import socket
import threading
import queue

from infinispan import error


class SocketConnection(object):
    def __init__(self, host="127.0.0.1", port=11222, timeout=10):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._s = None
        self.lock = threading.Lock()

    def connect(self):
        if self._s:
            raise error.ConnectionError("Already connected.")
        self._s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._s.connect((self.host, self.port))
            self._s.setblocking(0)
        except socket.error:
            self._s = None
            raise error.ConnectionError("Connection refused.")

    def send(self, byte_array):
        if not self._s:
            raise error.ConnectionError("Not connected.")

        try:
            ret = self._s.send(byte_array)
        except socket.error:
            raise error.ConnectionError("Socket connection broken.")
        if ret == 0:
            raise error.ConnectionError("Socket connection broken.")

    def recv(self):
        if not self._s:
            raise error.ConnectionError("Not connected.")

        n = 1
        while n != 0:
            try:
                packet = self._read_packet(n, self.timeout)
            except socket.error:
                raise error.ConnectionError(
                    "Connection reset by peer.")
            if not packet:
                raise error.ConnectionError(
                    "The remote end hung up unexpectedly.")
            n = yield packet
            # must test for None as 0 is termination
            n = n if n is not None else 1

    def disconnect(self):
        if not self._s:
            raise error.ConnectionError("Not connected.")
        try:
            self._s.shutdown(0)
        except socket.error:
            # TODO: Logging
            pass
        self._s.close()
        self._s = None

    @property
    def connected(self):
        return self._s is not None

    def _read_packet(self, n, timeout):
        packet = None
        delay = 50.0 / 1000.0
        curr_time = time.time()
        try_again = True
        while try_again:
            try_again = False
            try:
                packet = self._s.recv(n)
            except socket.error as ex:
                if ex.errno != 11:
                    raise ex
                try_again = True

                if time.time() - curr_time > timeout:
                    raise error.ConnectionError("Connection timeout.")
                time.sleep(delay)
                if delay < 0.4:
                    delay *= 2
        return packet


class ConnectionPool(object):
    def __init__(self, connections=[]):
        self._connections = connections
        self.size = len(self._connections)
        self._queue = queue.Queue(maxsize=self.size)
        for conn in self._connections:
            self._queue.put(conn)
        self.lock = threading.BoundedSemaphore(self.size)

    def connect(self):
        for conn in self._connections:
            conn.connect()

    def send(self, byte_array):
        conn = self._get_conn()
        try:
            conn.send(byte_array)
        finally:
            self._queue.task_done()
            self._return_conn(conn)

    def recv(self):
        conn = self._get_conn()
        data = conn.recv()
        try:
            n = yield next(data)
            while n != 0:
                n = yield data.send(n)
        finally:
            self._queue.task_done()
            self._return_conn(conn)

    def disconnect(self):
        for conn in self._connections:
            conn.disconnect()

    def _get_conn(self):
        return self._queue.get(False)

    def _return_conn(self, conn):
        self._queue.put(conn, False)

    @property
    def available(self):
        return self._queue.qsize()

    @property
    def connected(self):
        return all(map(lambda c: c.connected, self._connections))
