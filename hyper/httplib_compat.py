# -*- coding: utf-8 -*-
import socket
try:
    import http.client as httplib
except ImportError:
    import httplib

from .compat import ssl
from .http20.tls import wrap_socket

try:
    support_20 = ssl.HAS_NPN
except AttributeError:
    support_20 = False

HTTPConnection = httplib.HTTPConnection
HTTPSConnection = httplib.HTTPSConnection

if support_20:
    class HTTPSConnection(object):
        def __init__(self, *args, **kwargs):
            self._original_args = args
            self._original_kwargs = kwargs

            self._sock = None
            self._conn = None

            self._call_queue = []

        def __getattr__(self, name):
            delay_methods = ["set_tunnel", "set_debuglevel"]

            if self._conn is None and name in delay_methods:
                def capture(obj, *args, **kwargs):
                    pass
                return capture
            elif self._conn is None:
                self._delayed_connect()

            return getattr(self._conn, name)

        def _delayed_connect(self):
            pass
