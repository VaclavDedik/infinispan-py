# -*- coding: utf-8 -*-

import time
import socket
import threading

from collections import deque

from infinispan import error
from contextlib import contextmanager


class SocketConnection(object):
    def __init__(self, host="127.0.0.1", port=11222, timeout=10):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._s = None
        self._lock = threading.Lock()

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
        self._lock.acquire()

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
        self._lock.release()

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

    @contextmanager
    def context(self):
        try:
            yield self
        finally:
            if self._lock.locked():
                self._lock.release()

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
        self._lock = threading.Lock()
        self._curr = 0

    def connect(self):
        for conn in self._connections:
            conn.connect()

    def disconnect(self):
        for conn in self._connections:
            conn.disconnect()

    @property
    def connected(self):
        return all(map(lambda c: c.connected, self._connections))

    @property
    def size(self):
        return len(self._connections)

    @contextmanager
    def context(self):
        conn = self._get_next()
        try:
            yield conn
        finally:
            if conn._lock.locked():
                conn._lock.release()

    def _get_next(self):
        with self._lock:
            if self._curr >= self.size - 1:
                self._curr = 0
            else:
                self._curr += 1
            return self._connections[self._curr]
