
import hotrod
import connection

from hotrod import Status


class Infinispan(object):
    def __init__(self, host="127.0.0.1", port=11222):
        conn = connection.SocketConnection(host, port)
        self.protocol = hotrod.Protocol(conn)

    def get(self, key):
        req = hotrod.GetRequest(key=key)
        resp = self.protocol.send(req)
        if resp.header.status != Status.OK:
            return None
        else:
            return resp.value

    def put(self, key, value):
        req = hotrod.PutRequest(key=key, value=value)
        resp = self.protocol.send(req)
        return resp.prev_value

    def contians_key(self, key):
        pass

    def remove(self, key):
        pass
