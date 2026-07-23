# -*- coding: utf-8 -*-
from ..common.exceptions import ConnectionError as CommonConnectionError


class HTTP20Error(Exception):
    pass


class HPACKEncodingError(HTTP20Error):
    pass


class HPACKDecodingError(HTTP20Error):
    pass


class ConnectionError(CommonConnectionError, HTTP20Error):
    pass


class ProtocolError(HTTP20Error):
    pass


class StreamResetError(HTTP20Error):
    pass
