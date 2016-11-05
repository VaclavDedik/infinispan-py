
from infinispan import protocol, connection


class Infinispan(object):
    def __init__(self, host="127.0.0.1", port=11222):
        conn = connection.Connection(host, port)
        self.protocol = protocol.Protocol(conn)

    def get(self, key):
        pass

    def put(self, key, value):
        pass

    def contians_key(self, key):
        pass

    def remove(self, key):
        pass
