
from infinispan import protocol, connection


class Infinispan(object):
    def __init__(self, host="127.0.0.1", port=11222):
        conn = connection.Connection(host, port)
        self.protocol = protocol.Protocol(conn)

    def get(self, key):
        return protocol.get(key)

    def put(self, key, value):
        return protocol.put(key, value)

    def contians_key(self, key):
        return protocol.contains_key(key)

    def remove(self, key):
        return protocol.remove(key)
