
import hotrod
import connection
import exception

from hotrod import Status


class Infinispan(object):
    def __init__(self, host="127.0.0.1", port=11222, cache_name=None):
        self.conn = connection.SocketConnection(host, port)
        self.protocol = hotrod.Protocol(self.conn)
        self.cache_name = cache_name

    def get(self, key):
        req = hotrod.GetRequest(key=key)
        resp = self._send(req)
        if resp.header.status != Status.OK:
            return None
        else:
            return resp.value

    def put(self, key, value):
        req = hotrod.PutRequest(key=key, value=value)
        resp = self._send(req)
        return resp.prev_value

    def contians_key(self, key):
        pass

    def remove(self, key):
        pass

    def disconnect(self):
        if self.conn.connected:
            self.conn.disconnect()

    def _send(self, req):
        req.header.cname = self.cache_name
        resp = self.protocol.send(req)

        # Test if not an error response
        if isinstance(resp, hotrod.ErrorResponse):
            if resp.header.status in [Status.UNKNOWN_CMD,
                                      Status.UNKNOWN_VERSION,
                                      Status.PARSING_ERR]:
                raise exception.ClientError(resp.error_message, resp)
            elif resp.header.status in [Status.SERVER_ERR, Status.CMD_TIMEOUT]:
                raise exception.ServerError(resp.error_message, resp)
            else:
                raise exception.ResponseError(resp.error_message, resp)

        return resp

    def __enter__(self):
        if not self.conn.connected:
            self.conn.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.disconnect()
