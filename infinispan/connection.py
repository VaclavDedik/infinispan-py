# -*- coding: utf-8 -*-

import time
import socket
import threading

from infinispan import exception


class SocketConnection(object):
    def __init__(self, host="127.0.0.1", port=11222, timeout=10):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._s = None
        self.lock = threading.Lock()

    def connect(self):
        if self._s:
            raise exception.ConnectionError("Already connected.")
        self._s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._s.connect((self.host, self.port))
            self._s.setblocking(0)
        except socket.error:
            self._s = None
            raise exception.ConnectionError("Connection refused.")

    def send(self, byte_array):
        if not self._s:
            raise exception.ConnectionError("Not connected.")

        try:
            ret = self._s.send(byte_array)
        except socket.error:
            raise exception.ConnectionError("Socket connection broken.")
        if ret == 0:
            raise exception.ConnectionError("Socket connection broken.")

    def recv(self, n=1):
        if not self._s:
            raise exception.ConnectionError("Not connected.")

        while True:
            try:
                packet = self._read_packet(n, self.timeout)
            except socket.error:
                raise exception.ConnectionError(
                    "Connection reset by peer.")
            if not packet:
                raise exception.ConnectionError(
                    "The remote end hung up unexpectedly.")
            yield packet

    def disconnect(self):
        if not self._s:
            raise exception.ConnectionError("Not connected.")
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
                    raise exception.ConnectionError("Connection timeout.")
                time.sleep(delay)
                if delay < 0.4:
                    delay *= 2
        return packet
