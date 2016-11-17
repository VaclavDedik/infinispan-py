# -*- coding: utf-8 -*-

from infinispan import hotrod
from infinispan import connection
from infinispan import exception
from infinispan import utils
from infinispan import serial
from infinispan.hotrod import Status, Flag


class Infinispan(object):
    def __init__(self, host="127.0.0.1", port=11222, cache_name=None,
                 key_serial=None, val_serial=None):
        self.conn = connection.SocketConnection(host, port)
        self.protocol = hotrod.Protocol(self.conn)
        self.cache_name = cache_name

        self.key_serial = key_serial if key_serial else serial.JSONPickle()
        self.val_serial = val_serial if val_serial else serial.JSONPickle()

    def get(self, key):
        req = hotrod.GetRequest(key=self.key_serial.serialize(key))
        resp = self._send(req)
        return self.val_serial.deserialize(resp.value)

    def put(self, key, value, lifespan=None, max_idle=None, previous=False):
        req = hotrod.PutRequest(
            key=self.key_serial.serialize(key),
            value=self.val_serial.serialize(value))

        if lifespan:
            req.lifespan, req.tunits[0] = utils.from_pretty_time(lifespan)
        if max_idle:
            req.max_idle, req.tunits[1] = utils.from_pretty_time(max_idle)
        if previous:
            req.header.flags |= Flag.FORCE_RETURN_VALUE

        resp = self._send(req)
        return self.val_serial.deserialize(resp.prev_value)

    def contains_key(self, key):
        req = hotrod.ContainsKeyRequest(key=self.key_serial.serialize(key))
        resp = self._send(req)
        return resp.header.status == Status.OK

    def remove(self, key, previous=False):
        req = hotrod.RemoveRequest(key=self.key_serial.serialize(key))

        if previous:
            req.header.flags |= Flag.FORCE_RETURN_VALUE

        resp = self._send(req)
        return self.val_serial.deserialize(resp.prev_value)

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
