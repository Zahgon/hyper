# -*- coding: utf-8 -*-
from .exceptions import TLSUpgrade, HTTPUpgrade
from ..http11.connection import HTTP11Connection
from ..http20.connection import HTTP20Connection
from ..tls import H2_NPN_PROTOCOLS, H2C_PROTOCOL


class HTTPConnection(object):
    def __init__(self,
                 host,
                 port=None,
                 secure=None,
                 window_manager=None,
                 enable_push=False,
                 ssl_context=None,
                 proxy_host=None,
                 proxy_port=None,
                 proxy_headers=None,
                 timeout=None,
                 **kwargs):

        self._host = host
        self._port = port
        self._h1_kwargs = {
            'secure': secure, 'ssl_context': ssl_context,
            'proxy_host': proxy_host, 'proxy_port': proxy_port,
            'proxy_headers': proxy_headers, 'enable_push': enable_push,
            'timeout': timeout
        }
        self._h2_kwargs = {
            'window_manager': window_manager, 'enable_push': enable_push,
            'secure': secure, 'ssl_context': ssl_context,
            'proxy_host': proxy_host, 'proxy_port': proxy_port,
            'proxy_headers': proxy_headers,
            'timeout': timeout
        }

        self._h1_kwargs.update(kwargs)
        self._h2_kwargs.update(kwargs)

        self._conn = HTTP11Connection(
            self._host, self._port, **self._h1_kwargs
        )

    def request(self, method, url, body=None, headers=None):
        """
        This will send a request to the server using the HTTP request method
        ``method`` and the selector ``url``. If the ``body`` argument is
        present, it should be string or bytes object of data to send after the
        headers are finished. Strings are encoded as UTF-8. To use other
        encodings, pass a bytes object. The Content-Length header is set to the
        length of the body field.

        :param method: The request method, e.g. ``'GET'``.
        :param url: The URL to contact, e.g. ``'/path/segment'``.
        :param body: (optional) The request body to send. Must be a bytestring
            or a file-like object.
        :param headers: (optional) The headers to send on the request.
        :returns: A stream ID for the request, or ``None`` if the request is
            made over HTTP/1.1.
        """

        headers = headers or {}

        try:
            return self._conn.request(
                method=method, url=url, body=body, headers=headers
            )
        except TLSUpgrade as e:
            assert e.negotiated in H2_NPN_PROTOCOLS

            self._conn = HTTP20Connection(
                self._host, self._port, **self._h2_kwargs
            )
            self._conn._sock = e.sock

            self._conn._send_preamble()

            return self._conn.request(
                method=method, url=url, body=body, headers=headers
            )

    def get_response(self, *args, **kwargs):
        """
        Returns a response object.
        """
        try:
            return self._conn.get_response(*args, **kwargs)
        except HTTPUpgrade as e:
            assert e.negotiated == H2C_PROTOCOL

            self._conn = HTTP20Connection(
                self._host, self._port, **self._h2_kwargs
            )

            self._conn._connect_upgrade(e.sock)

            return self._conn.get_response(1)

    def __enter__(self):  # pragma: no cover
        return self

    def __exit__(self, type, value, tb):  # pragma: no cover
        self._conn.close()
        return False  # Never swallow exceptions.

    def __getattr__(self, name):
        return getattr(self._conn, name)
