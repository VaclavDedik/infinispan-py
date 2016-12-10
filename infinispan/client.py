# -*- coding: utf-8 -*-

import threading
import logging

from concurrent.futures import ThreadPoolExecutor

from infinispan import hotrod
from infinispan import connection
from infinispan import error
from infinispan import utils
from infinispan import serial
from infinispan.async import generate_async, sync_op
from infinispan.hotrod import Status, Flag, ClientIntelligence

log = logging.getLogger(__name__)


@generate_async
class Infinispan(object):
    """Main infinispan-py client interface. Use this interface if you want to
    work with a high level of abstraction. For a low level of abstraction, see
    module :mod:`infinispan.hotrod`.

    This class implements context manager interface, which opens a socket
    connection upon entry and closes it when exited. Alternatively, you can
    open and close the connection youself with methods :meth:`connect` and
    :meth:`disconnect`. When used out of any context manager or when connection
    is not manually opened via :meth:`connect`, connection is automatically
    established when a connection bound method is invoked for the first time.

    You can also use asynchronous counterparts of every connection bound
    blocking operation (like :meth:`get`, :meth:`put` etc) by calling the name
    of the method you want with '_async' suffix, e.g. if you want to call
    :meth:`put` asynchronously, instad of invoking just :meth:`put`, you invoke
    'put_async'. Async operations return :class:`concurrent.futures.Future`.
    """

    def __init__(self, host="127.0.0.1", port=11222, timeout=10,
                 cache_name=None, key_serial=None, val_serial=None,
                 pool_size=20):
        """Initializes new client instance.

        :param host: IP address of the host where Infinispan server is running.
                     Default value is "127.0.0.1".
        :param port: Port number of the Infinispan server.
                     Default value is 11222.
        :param timeout: How long should the client wait for a response from the
                        server in seconds. Default value is 10 seconds.
        :param cache_name: Specify a name if named cache should be used.
        :param key_serial: Serializer of the key. A serializer must implement
                           class :class:`infinispan.serial.Serialization`.
                           By default, :class:`infinispan.serial.JSONPickle` is
                           used.
        :param val_serial: Same as key_serial, but for value.
        :param pool_size: Determines the thread pool size that is used for
                          async operations.
        """

        log.info("Initializing client with host=%r, port=%r, timeout=%r, "
                 "cache_name=%r, key_serial=%r, val_serial=%r, pool_size=%r",
                 host, port, timeout, cache_name, key_serial, val_serial,
                 pool_size)

        self.conn_type = connection.SocketConnection
        conn = connection.ConnectionPool(connections=[
            self.conn_type(host, port, timeout=timeout)])
        self.protocol = hotrod.Protocol(conn, timeout=timeout)
        self.cache_name = cache_name
        self.ci = ClientIntelligence.TOPOLOGY

        self.key_serial = key_serial if key_serial else serial.JSONPickle()
        self.val_serial = val_serial if val_serial else serial.JSONPickle()

        self.executor = ThreadPoolExecutor(max_workers=pool_size)

        self._lock = threading.Lock()
        self._curr_topology_id = 0

    @sync_op
    def get(self, key):
        """Sends a request to Infinispan server asking for a value by key.

        :param key: Key associated with the value you want to retrieve.
        :return: Value associated with the key if key exists,
                  :obj:`None` otherwise.
        """
        req = hotrod.GetRequest(key=self.key_serial.serialize(key))
        resp = self._send(req)
        return self.val_serial.deserialize(resp.value)

    @sync_op
    def put(self, key, value, lifespan=None, max_idle=None, previous=False):
        """Creates new key-value pair on the Infinispan server.

        :param key: Key to be associated with a value.
        :param value: Value to be associated with the key.
        :param lifespan: How long should the key-value pair be stored on the
                         server. Accepts value in the following format: '$n$u',
                         where $n is number (e.g. 10) and $u is unit, which can
                         be 's' (second), 'ms' (milisecond),
                         'us' (microsecond), 'ns' (nanosecond), 'm' (minute),
                         'h' (hour) and 'd' (day). For example '10m' means the
                         server will wait 10 minutes before removing the
                         key-value pair. You can also use 'inf' value, which
                         means the key-value pair will be available forever.
                         By default, infinispan server configuration is used.
        :param max_idle: How long can this key-value pair be idle (no clients
                         requests for it) before it is removed from the server.
                         For the accepted format and default value,
                         see :attr:`lifespan`.
        :param previous: Force return of previously stored value under the key.
        :return: :obj:`None` unless previous value forced.
        """
        req = hotrod.PutRequest(
            key=self.key_serial.serialize(key),
            value=self.val_serial.serialize(value))

        resp = self._send(req, lifespan=lifespan, max_idle=max_idle,
                          previous=previous)
        return self.val_serial.deserialize(resp.prev_value)

    @sync_op
    def put_if_absent(self, key, value, lifespan=None, max_idle=None,
                      previous=False):
        """Creates new key-value pair on the Infinispan server if absent.

        :param key: Key to be associated with a value.
        :param value: Value to be associated with the key.
        :param lifespan: How long should the key-value pair be stored on the
                         server. See :meth:`put` for detials.
        :param max_idle: How long can this key-value pair be idle (no clients
                         requests for it) before it is removed from the server.
                         See :meth:`put for details`.
        :param previous: Force return of previously stored value under the key.
        :return: :obj:`True` if key absent, :obj:`False` otherwise.
                 If previous value forced, returns :obj:`None` if key absent,
                 previous value otherwise.
        """
        req = hotrod.PutIfAbsentRequest(
            key=self.key_serial.serialize(key),
            value=self.val_serial.serialize(value))

        resp = self._send(req, lifespan=lifespan, max_idle=max_idle,
                          previous=previous)
        if previous:
            return self.val_serial.deserialize(resp.prev_value)
        else:
            return resp.header.status == Status.OK

    @sync_op
    def replace(self, key, value, lifespan=None, max_idle=None,
                previous=False):
        """Replaces existing key-value pair on the Infinispan server if absent.

        :param key: Key to be replaced.
        :param value: Value to be replaced.
        :param lifespan: How long should the key-value pair be stored on the
                         server. See :meth:`put` for detials.
        :param max_idle: How long can this key-value pair be idle (no clients
                         requests for it) before it is removed from the server.
                         See :meth:`put for details`.
        :param previous: Force return of previously stored value under the key.
        :return: :obj:`True` if key replaced, :obj:`False` otherwise.
                 If previous value forced, returns previous value if key
                 replaced, :obj:`None` otherwise.
        """
        req = hotrod.ReplaceRequest(
            key=self.key_serial.serialize(key),
            value=self.val_serial.serialize(value))

        resp = self._send(req, lifespan=lifespan, max_idle=max_idle,
                          previous=previous)
        if previous:
            return self.val_serial.deserialize(resp.prev_value)
        else:
            return resp.header.status == Status.OK

    @sync_op
    def contains_key(self, key):
        """Returns whether the key is stored on the server.

        :param key: Key you want to know if available on the server.
        :return: :obj:`True` if key is stored on the server,
                  :obj:`False` otherwise."""
        req = hotrod.ContainsKeyRequest(key=self.key_serial.serialize(key))
        resp = self._send(req)
        return resp.header.status == Status.OK

    @sync_op
    def remove(self, key, previous=False):
        """Removes key and it's associated value from the server.

        :param key: Key you want to remove.
        :param previous: Force return of previously stored value under the key.
        :return: :obj:`None` unless previous value forced.
        """
        req = hotrod.RemoveRequest(key=self.key_serial.serialize(key))

        resp = self._send(req, previous=previous)
        return self.val_serial.deserialize(resp.prev_value)

    @sync_op
    def ping(self):
        """Pings the server to test the connection.

        :return: :obj:`True` if response status is OK."""
        req = hotrod.PingRequest()
        resp = self._send(req)
        return resp.header.status == Status.OK

    def connect(self):
        """Establishes connection with the server. If connection is already
        open, does not do anything."""

        with self._lock:
            if not self.protocol.conn.connected:
                self.protocol.conn.connect()

    def disconnect(self):
        """Closes connection with the server. If connection is already closed,
        does not do anything."""

        with self._lock:
            if self.protocol.conn.connected:
                self.protocol.conn.disconnect()

    def _send(self, req, lifespan=None, max_idle=None, previous=False):
        if not self.protocol.conn.connected:
            self.connect()

        self._set_ephemeral_props(req, lifespan, max_idle)
        self._set_flags(req, previous=previous)

        log.debug("Sending request of type %s", req.__class__.__name__)

        req.header.cname = self.cache_name
        req.header.ci = self.ci
        req.header.t_id = self._curr_topology_id
        resp = self.protocol.send(req)

        log.debug("Received response of type %s", resp.__class__.__name__)

        # Test if not an error response
        if isinstance(resp, hotrod.ErrorResponse):
            log.error("Retrieved error response with message '%s'",
                      resp.error_message)
            raise error.ClientError(resp.error_message, resp)

        # Test if topology changed:
        if resp.header.tcm:
            log.info("Topology changed, updating.")
            self._handle_topology_change(resp)

        return resp

    def _handle_topology_change(self, response):
        with self._lock:
            if response.header.tc.id != self._curr_topology_id:
                self._curr_topology_id = response.header.tc.id
                conns = [self.conn_type(host.ip, host.port,
                                        timeout=self.protocol.timeout)
                         for host in response.header.tc.hosts]
                self.protocol.conn.update(conns)

    def _set_ephemeral_props(self, req, lifespan=None, max_idle=None):
        if lifespan:
            req.lifespan, req.tunits[0] = utils.from_pretty_time(lifespan)
        if max_idle:
            req.max_idle, req.tunits[1] = utils.from_pretty_time(max_idle)

    def _set_flags(self, req, flags=None, previous=False):
        flags = set([]) if flags is None else flags
        if previous:
            flags.add(Flag.FORCE_RETURN_VALUE)
        for flag in flags:
            req.header.flags |= flag

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.disconnect()
