
import socket


class SocketConnection(object):
    def __init__(self, host="127.0.0.1", port=11222):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((host, port))

    def send(self, byte_array):
        self.s.send(byte_array)

    def recv(self, n=1):
        while True:
            packet = self.s.recv(n)
            yield packet

    def close(self):
        self.s.shutdown()
        self.s.close()
