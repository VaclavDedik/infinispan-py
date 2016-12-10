# -*- coding: utf-8 -*-

import time
import threading
import logging

from collections import OrderedDict

from infinispan import messenger as m
from infinispan import codec
from infinispan import error

log = logging.getLogger(__name__)


class ClientIntelligence(object):
    BASIC = 0x01
    TOPOLOGY = 0x02
    HASH = 0x03


class TimeUnits(object):
    SECONDS = 0x00
    MILISECONDS = 0x01
    NANOSECONDS = 0x02
    MICROSECONDS = 0x03
    MINUTES = 0x04
    HOURS = 0x05
    DAYS = 0x06
    DEFAULT = 0x07
    INFINITE = 0x08


class Status(object):
    OK = 0x00
    ACTION_FAILED = 0x01
    KEY_DOES_NOT_EXISTS = 0x02
    OK_WITH_VALUE = 0x03
    FAIL_WITH_VALUE = 0x04
    OK_COMP_ENABLED = 0x06
    OK_PREV_VAL_COMP_ENABLED = 0x07
    NOT_EXEC_PREV_VAL_COMP_ENABLED = 0x08
    INVALID_MSGID_OR_MAGIC = 0x81
    UNKNOWN_CMD = 0x82
    UNKNOWN_VERSION = 0x83
    PARSING_ERR = 0x84
    SERVER_ERR = 0x85
    CMD_TIMEOUT = 0x86


class Flag(object):
    FORCE_RETURN_VALUE = 0x0001
    DEFAULT_LIFESPAN = 0x0002
    DEFAULT_MAXIDLE = 0x0004
    SKIP_CACHE_LOAD = 0x0008
    SKIP_INDEXING = 0x0010


class RequestHeader(m.Message):
    magic = m.Byte(default=0xA0)
    id = m.Uvarlong()
    version = m.Byte(default=25)
    op = m.Byte()
    cname = m.String(optional=True)
    flags = m.Uvarint(default=0)
    ci = m.Byte(default=ClientIntelligence.BASIC)
    t_id = m.Uvarint(default=0)


class Host(m.Message):
    ip = m.String(default="127.0.0.1")
    port = m.Ushort(default=11211)


class TopologyChangeHeader(m.Message):
    id = m.Uvarint()
    n = m.Uvarint()
    hosts = m.List(of=Host, size=lambda s: s.n)


class ResponseHeader(m.Message):
    magic = m.Byte(default=0xA1)
    id = m.Uvarlong()
    op = m.Byte()
    status = m.Byte(default=Status.OK)
    tcm = m.Byte(default=0)
    tc = m.Composite(default=TopologyChangeHeader, condition=lambda s: s.tcm)


class Request(m.Message):
    header = m.Composite(default=RequestHeader)

    def __init__(self, **kwargs):
        super(Request, self).__init__(**kwargs)
        self.header.op = self.OP_CODE


class Response(m.Message):
    header = m.Composite(default=ResponseHeader)

    def __init__(self, **kwargs):
        super(Response, self).__init__(**kwargs)
        self.header.op = self.OP_CODE


class GetRequest(Request):
    OP_CODE = 0x03
    key = m.Bytes()


class GetResponse(Response):
    OP_CODE = 0x04
    value = m.Bytes(condition=lambda s: s.header.status == Status.OK)


class PutRequest(Request):
    OP_CODE = 0x01
    key = m.Bytes()
    tunits = m.SplitByte(default=[TimeUnits.DEFAULT, TimeUnits.DEFAULT])
    lifespan = m.Uvarint(default=10, condition=lambda s: s.tunits[0] not in
                         [TimeUnits.DEFAULT, TimeUnits.INFINITE])
    max_idle = m.Uvarint(default=10, condition=lambda s: s.tunits[1] not in
                         [TimeUnits.DEFAULT, TimeUnits.INFINITE])
    value = m.Bytes()


class PutIfAbsentRequest(PutRequest):
    OP_CODE = 0x05


class PutResponse(Response):
    OP_CODE = 0x02
    prev_value = m.Bytes(
        condition=lambda s: s.header.status == Status.OK_WITH_VALUE)


class PutIfAbsentResponse(Response):
    OP_CODE = 0x06
    prev_value = m.Bytes(
        condition=lambda s: s.header.status == Status.FAIL_WITH_VALUE)


class PingRequest(Request):
    OP_CODE = 0x17


class PingResponse(Response):
    OP_CODE = 0x18


class ErrorResponse(Response):
    OP_CODE = 0x50
    error_message = m.String()


class RemoveRequest(Request):
    OP_CODE = 0x0B
    key = m.Bytes()


class RemoveResponse(Response):
    OP_CODE = 0x0C
    prev_value = m.Bytes(
        condition=lambda s: s.header.status == Status.OK_WITH_VALUE)


class ContainsKeyRequest(Request):
    OP_CODE = 0x0F
    key = m.Bytes()


class ContainsKeyResponse(Response):
    OP_CODE = 0x10


class Protocol(object):
    """Low level API that sends requests and blocks until response received."""

    def __init__(self, conn, timeout=10):
        """Creates new protocol instance.

        :param conn: Connection, you need to open the connection yourself
                     before you can send requests and close it when you are
                     done.
        """
        self.lock = threading.Lock()
        self.conn = conn
        self.timeout = timeout
        self._id = 0
        self._responses = OrderedDict()
        self._decoder_f = codec.DecoderFactory()
        self._encoder_f = codec.EncoderFactory()

    def send(self, request):
        """Sends a request to the server.

        :param request: Request to be sent to the associated Infinispan server.
        :return: Response from the server.
        """

        # encode request
        req_id = self._get_next_id()
        request.header.id = req_id
        encoder = self._encoder_f.get()
        encoded_request = encoder.encode(request)

        # send request and wait until received the correct response
        with self.conn.context() as ctx:
            log.debug("Sending request id=%r encoded as %r to %s",
                      req_id, encoded_request, ctx)
            ctx.send(encoded_request)
            # receive and decode
            data = ctx.recv()
            decoder = self._decoder_f.get()
            response = decoder.decode(data)
            log.debug("Received response id=%r from %s",
                      response.header.id, ctx)
            self._cache_resp(response)

        mustend = time.time() + self.timeout
        while req_id not in self._responses:
            # if there is an error response in the cache, raise an error
            err_resp = self._pop_resp(0)
            if err_resp:
                log.error("Received server error without id, message: %s",
                          err_resp.error_message)
                raise error.ServerError(err_resp.error_message, err_resp)

            time.sleep(0.005)
            if time.time() > mustend:
                log.error("Timeout waiting on response with id=%r", req_id)
                raise error.ConnectionError("Timeout.")
        return self._pop_resp(req_id)

    def _cache_resp(self, response):
        with self.lock:
            if len(self._responses) > 100:
                self._responses = dict(
                    self._responses.items()[:len(self._responses)/2])
            self._responses[response.header.id] = response

    def _pop_resp(self, id):
        with self.lock:
            if id in self._responses:
                resp = self._responses[id]
                del self._responses[id]
                return resp

    def _get_next_id(self):
        with self.lock:
            if self._id > 9223372036854775808:
                self._id = 0
            self._id += 1
            return self._id
