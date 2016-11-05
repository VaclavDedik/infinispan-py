
import messenger as m

from encoder import Encoder, Decoder


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
    OK_COMP_ENABLED = 0x06
    OK_PREV_VAL_COMP_ENABLED = 0x07
    NOT_EXEC_PREV_VAL_COMP_ENABLED = 0x08
    INVALID_MSGID_OR_MAGIC = 0x81
    UNKNOWN_CMD = 0x82
    UNKNOWN_VERSION = 0x83
    PARSING_ERR = 0x84
    SERVER_ERR = 0x85
    CMD_TIMEOUT = 0x86


class RequestHeader(m.Message):
    magic = m.Byte(default=0xA0)
    id = m.Uvarlong()
    version = m.Byte(default=25)
    op = m.Byte()
    cname = m.Lenstr()
    flags = m.Uvarint(default=0)
    ci = m.Byte(default=ClientIntelligence.BASIC)
    t_id = m.Uvarint(default=0)


class ResponseHeader(m.Message):
    magic = m.Byte(default=0xA1)
    id = m.Uvarlong()
    op = m.Byte()
    status = m.Uvarint(default=Status.OK)
    tcm = m.Uvarint(default=0)


class Request(m.Message):
    header = m.Composite(default=RequestHeader())

    def __init__(self, **kwargs):
        super(Request, self).__init__(**kwargs)
        self.header.op = self.OP_CODE


class Response(m.Message):
    header = m.Composite(default=ResponseHeader())

    def __init__(self, **kwargs):
        super(Response, self).__init__(**kwargs)
        self.header.op = self.OP_CODE


class GetRequest(Request):
    OP_CODE = 0x03
    key = m.Lenstr()


class GetResponse(Response):
    OP_CODE = 0x04
    value = m.Lenstr(condition=lambda s: s.header.status == Status.OK)


class PutRequest(Request):
    OP_CODE = 0x01
    key = m.Lenstr()
    tunits = m.Byte(default=TimeUnits.DEFAULT)
    lifespan = m.Uvarint(default=10, condition=lambda s: s.tunits not in
                         [TimeUnits.DEFAULT, TimeUnits.INFINITE])
    max_idle = m.Uvarint(default=10, condition=lambda s: s.tunits not in
                         [TimeUnits.DEFAULT, TimeUnits.INFINITE])
    value = m.Lenstr()


class PutResponse(Request):
    OP_CODE = 0x02


class Protocol(object):
    def __init__(self, conn):
        self.conn = conn

    def send(self, request):
        encoded_request = self.encode(request, Encoder()).result()
        self.conn.send(encoded_request)
        data = self.conn.recv()
        response = GetResponse()
        self.decode(response, Decoder(data))
        return response

    def encode(self, message, encoder):
        for f_name in message.fields:
            f = getattr(message, f_name)
            f_cls = getattr(message.__class__, f_name)

            if 'condition' in dir(f_cls) and not f_cls.condition(message):
                continue

            if f_cls.type == "composite":
                encoder = self.encode(f, encoder)
            else:
                getattr(encoder, f_cls.type)(f)
        return encoder

    def decode(self, message, decoder):
        for f_name in message.fields:
            f = getattr(message, f_name)
            f_cls = getattr(message.__class__, f_name)

            if 'condition' in dir(f_cls) and not f_cls.condition(message):
                continue

            if f_cls.type == "composite":
                decoder = self.decode(f, decoder)
            else:
                decoded = getattr(decoder, f_cls.type)()
                setattr(message, f_name, decoded)
        return decoder
