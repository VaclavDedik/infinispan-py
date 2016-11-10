
import threading
import messenger as m
import exception

from codec import Encoder, Decoder


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
    cname = m.Lenstr(default='')
    flags = m.Uvarint(default=0)
    ci = m.Byte(default=ClientIntelligence.BASIC)
    t_id = m.Uvarint(default=0)


class ResponseHeader(m.Message):
    magic = m.Byte(default=0xA1)
    id = m.Uvarlong()
    op = m.Byte()
    status = m.Byte(default=Status.OK)
    tcm = m.Byte(default=0)


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
    tunits = m.SplitByte(default=[TimeUnits.DEFAULT, TimeUnits.DEFAULT])
    lifespan = m.Uvarint(default=10, condition=lambda s: s.tunits[0] not in
                         [TimeUnits.DEFAULT, TimeUnits.INFINITE])
    max_idle = m.Uvarint(default=10, condition=lambda s: s.tunits[1] not in
                         [TimeUnits.DEFAULT, TimeUnits.INFINITE])
    value = m.Lenstr()


class PutResponse(Response):
    OP_CODE = 0x02
    prev_value = m.Lenstr(
        condition=lambda s: s.header.status == Status.OK_WITH_VALUE)


class PingRequest(Request):
    OP_CODE = 0x17


class PingResponse(Response):
    OP_CODE = 0x18


class ErrorResponse(Response):
    OP_CODE = 0x50
    error_message = m.Lenstr()


class Protocol(object):
    def __init__(self, conn):
        self.lock = threading.Lock()
        self.conn = conn
        self._id = 0
        self._resps = {}
        # initiate connection
        self.conn.connect()

    def send(self, request):
        # encode request and send it
        req_id = self._get_next_id()
        request.header.id = req_id
        encoded_request = self.encode(request)
        self.conn.send(encoded_request)

        # wait until received the correct response
        while req_id not in self._resps:
            with self.lock:
                data = self.conn.recv()
                response = self.decode(data)
                self._resps[req_id] = response
        return self._resps[req_id]

    def encode(self, message):
        return self._encode(message, Encoder()).result()

    def decode(self, data):
        rh = ResponseHeader()
        decoder = Decoder(data)
        decoder = self._decode(rh, decoder)

        response = None
        for resp_cls in Response.__subclasses__():
            if hasattr(resp_cls, 'OP_CODE') and resp_cls.OP_CODE == rh.op:
                response = resp_cls(header=rh)

        if response is None:
            raise exception.DecodeError(
                "Response operation with code %s is not supported.", rh.op)
        self._decode(response, decoder, skip_fields=1)
        return response

    def _encode(self, message, encoder):
        for f_name in message.fields:
            f = getattr(message, f_name)
            f_cls = getattr(message.__class__, f_name)

            # test if field is available only under condition
            if hasattr(f_cls, 'condition') and not f_cls.condition(message):
                continue
            # test if field is none and raise an error if so
            if f is None:
                raise exception.EncodeError(
                    "Field '%s' of '%s#%s' must not be None",
                    f, type(message).__name__, f_name)

            if f_cls.type == "composite":
                encoder = self._encode(f, encoder)
            else:
                getattr(encoder, f_cls.type)(f)
        return encoder

    def _decode(self, message, decoder, skip_fields=0):
        for f_name in message.fields[skip_fields:]:
            f = getattr(message, f_name)
            f_cls = getattr(message.__class__, f_name)

            if 'condition' in dir(f_cls) and not f_cls.condition(message):
                continue

            if f_cls.type == "composite":
                decoder = self._decode(f, decoder)
            else:
                decoded = getattr(decoder, f_cls.type)()
                setattr(message, f_name, decoded)
        return decoder

    def _get_next_id(self):
        with self.lock:
            self._id += 1
            return self._id
