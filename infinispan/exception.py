

class DecodeError(Exception):
    pass


class EncodeError(Exception):
    pass


class ConnectionError(Exception):
    pass


class ResponseError(Exception):
    def __init__(self, message, response):
        super(ResponseError, self).__init__(message)
        self.response = response


class ClientError(ResponseError):
    pass


class ServerError(ResponseError):
    pass
