# -*- coding: utf-8 -*-


class ChunkedDecodeError(Exception):
    pass


class InvalidResponseError(Exception):
    pass


class SocketError(Exception):
    pass


class LineTooLongError(Exception):
    pass


try:  # pragma: no cover
    ConnectionResetError = ConnectionResetError
except NameError:  # pragma: no cover
    class ConnectionResetError(Exception):
        pass


class TLSUpgrade(Exception):
    def __init__(self, negotiated, sock):
        super(TLSUpgrade, self).__init__()
        self.negotiated = negotiated
        self.sock = sock


class HTTPUpgrade(Exception):
    def __init__(self, negotiated, sock):
        super(HTTPUpgrade, self).__init__()
        self.negotiated = negotiated
        self.sock = sock


class MissingCertFile(Exception):
    pass


try:  # pragma: no cover
    ConnectionError = ConnectionError
except NameError:  # pragma: no cover
    class ConnectionError(Exception):
        pass


class ProxyError(ConnectionError):
    def __init__(self, message, response):
        self.response = response
        super(ProxyError, self).__init__(message)
