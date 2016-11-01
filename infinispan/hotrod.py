
from encoder import Encoder, Decoder


class ClientInteligence(object):
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
    OK_COMP_ENABLED = 0x06
    OK_PREV_VAL_COMP_ENABLED = 0x07
    NOT_EXEC_PREV_VAL_COMP_ENABLED = 0x08
    INVALID_MSGID_OR_MAGIC = 0x81
    UNKNOWN_CMD = 0x82
    UNKNOWN_VERSION = 0x83
    PARSING_ERR = 0x84
    SERVER_ERR = 0x85
    CMD_TIMEOUT = 0x86


class RequestHeader(object):
    def __init__(self, id, version=25, cname=None, flags=0,
                 ci=ClientInteligence.BASIC, t_id=0):
        self.magic = 0xA0
        self.id = id
        self.version = version
        self.op = None
        self.cname = cname
        self.flags = flags
        self.ci = ci
        self.t_id = t_id

    def serialize(self):
        return Encoder() \
            .byte(self.magic) \
            .uintvar(self.id) \
            .byte(self.version) \
            .byte(self.op) \
            .len_str(self.cname) \
            .uintvar(self.flags) \
            .byte(self.ci) \
            .uintvar(self.t_id) \
            .encode()


class ResponseHeader(object):
    def __init__(self, id, status=Status.OK, tcm=0):
        self.magic = 0xA1
        self.id = id
        self.op = None
        self.status = status
        self.tcm = tcm

    def deserialize(self, byte_array):
        rh_dec = Decoder(byte_array)
        self.magic = rh_dec.byte()
        self.id = rh_dec.uintvar()
        self.op = rh_dec.byte()
        self.status = rh_dec.byte()
        self.tcm = rh_dec.byte()
        return rh_dec


class Request(object):
    def __init__(self, header):
        self.header = header

    def serialize(self):
        return self.header.serialize()


class Response(object):
    def __init__(self, header):
        self.header = header

    def deserialize(self, byte_gen):
        return self.header.deserialize(byte_gen)


class GetRequest(Request):
    OP_CODE = 0x03

    def __init__(self, header, key):
        super(GetRequest, self).__init__(header)
        self.header.op = self.OP_CODE
        self.key = key

    def serialize(self):
        return Encoder(super(GetRequest, self).serialize()) \
            .len_str(self.key) \
            .encode()


class GetResponse(Response):
    OP_CODE = 0x04

    def __init__(self, header, value=None):
        super(GetResponse, self).__init__(header)
        self.header.op = self.OP_CODE
        self.value = value

    def deserialize(self, byte_gen):
        rh_dec = super(GetResponse, self).deserialize(byte_gen)
        if self.header.status == Status.OK:
            self.value = rh_dec.len_str()
        return rh_dec


class PutRequest(Request):
    OP_CODE = 0x01

    def __init__(self, header, key, value, tunits=TimeUnits.DEFAULT,
                 lifespan=10, maxidle=10):
        super(PutRequest, self).__init__(header)
        self.header.op = self.OP_CODE
        self.key = key
        self.value = value
        self.tunits = tunits
        self.lifespan = lifespan
        self.maxidle = maxidle

    def serialize(self):
        rh_enc = Encoder(super(PutRequest, self).serialize()) \
            .len_str(self.key) \
            .byte(self.tunits)
        if self.tunits not in [TimeUnits.DEFAULT, TimeUnits.INFINITE]:
            rh_enc.uintvar(self.lifespan) \
                  .uintvar(self.maxidle)
        rh_enc.len_str(self.value)

        return rh_enc.encode()


class PutResponse(Request):
    OP_CODE = 0x02

    def __init__(self, header):
        super(PutResponse, self).__init__(header)
        self.header.op = self.OP_CODE

    def deserialize(self, byte_gen):
        rh_dec = super(PutResponse, self).deserialize(byte_gen)
        return rh_dec


class Protocol(object):
    def __init__(self, conn):
        self.conn = conn

    def send(self, request):
        id = request.header.id
        self.conn.send(request.serialize())
        data = self.conn.recv()
        response = GetResponse(ResponseHeader(id))
        response.deserialize(data)
        return response
