
import socket
import exception


class SocketConnection(object):
    def __init__(self, host="127.0.0.1", port=11222):
        self.host = host
        self.port = port
        self.s = None

    def connect(self):
        if self.s:
            raise exception.ConnectionError("Already connected.")
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.host, self.port))

    def send(self, byte_array):
        if not self.s:
            raise exception.ConnectionError("Not connected.")
        self.s.send(byte_array)

    def recv(self, n=1):
        if not self.s:
            raise exception.ConnectionError("Not connected.")
        while True:
            packet = self.s.recv(n)
            yield packet

    def disconnect(self):
        if not self.s:
            raise exception.ConnectionError("Not connected.")
        self.s.shutdown(0)
        self.s.close()
        self.s = None
