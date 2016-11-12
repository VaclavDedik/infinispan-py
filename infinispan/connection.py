
import socket

from infinispan import exception


class SocketConnection(object):
    def __init__(self, host="127.0.0.1", port=11222):
        self.host = host
        self.port = port
        self._s = None

    def connect(self):
        if self._s:
            raise exception.ConnectionError("Already connected.")
        self._s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._s.connect((self.host, self.port))

    def send(self, byte_array):
        if not self._s:
            raise exception.ConnectionError("Not connected.")
        self._s.send(byte_array)

    def recv(self, n=1):
        if not self._s:
            raise exception.ConnectionError("Not connected.")
        while True:
            packet = self._s.recv(n)
            if not packet:
                raise exception.ConnectionError(
                    "The remote end hung up unexpectedly")
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
