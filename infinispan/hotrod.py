
from encoder import Encoder, Decoder


class SendOp(object):
    PUT = 0x01
    GET = 0x03


class RecvOp(object):
    PUT = 0x02
    GET = 0x04


class ClientInteligence(object):
    BASIC = 0x01
    TOPOLOGY = 0x02
    HASH = 0x03


class Status(object):
    OK = 0x00


class RequestHeader(object):
    def __init__(self, id, version=25, op=SendOp.GET, cname=None,
                 ci=ClientInteligence.BASIC, t_id=0):
        self.magic = 0xA0
        self.id = id
        self.version = version
        self.op = op
        self.cname = cname
        self.ci = ci
        self.t_id = t_id

    def serialize(self):
        return Encoder() \
            .byte(self.magic) \
            .uintvar(self.id) \
            .byte(self.version) \
            .byte(self.op) \
            .len_str(self.cname) \
            .byte(0) \
            .byte(self.ci) \
            .byte(self.t_id) \
            .encode()


class ResponseHeader(object):
    def __init__(self, id, op=RecvOp.GET, status=Status.OK, tcm=0):
        self.magic = 0xA1
        self.id = id
        self.op = op
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
    def __init(self, header):
        self.header = header

    def deserialize(self, byte_array):
        return self.header.deserialize(byte_array)


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

    def deserialize(self, byte_array):
        rh_dec = super(GetResponse, self).deserialize(byte_array)
        self.value = rh_dec.len_str()
        return rh_dec


class Protocol(object):
    def __init__(self, conn):
        self.conn = conn

    def send(self, request):
        self.conn.send()
