# -*- coding: utf-8 -*-
import h2.connection
import h2.events
import h2.settings

from ..compat import ssl
from ..tls import wrap_socket, H2_NPN_PROTOCOLS, H2C_PROTOCOL
from ..common.exceptions import ConnectionResetError
from ..common.bufsocket import BufferedSocket
from ..common.headers import HTTPHeaderMap
from ..common.util import (
    to_host_port_tuple, to_native_string, to_bytestring, HTTPVersion
)
from ..compat import unicode, bytes
from ..http11.connection import _create_tunnel
from .stream import Stream
from .response import HTTP20Response, HTTP20Push
from .window import FlowControlManager
from .exceptions import ConnectionError, StreamResetError
from . import errors

import errno
import logging
import socket
import time
import threading
import itertools

log = logging.getLogger(__name__)

DEFAULT_WINDOW_SIZE = 65535

TRANSIENT_SSL_ERRORS = (ssl.SSL_ERROR_WANT_READ, ssl.SSL_ERROR_WANT_WRITE)


class _LockedObject(object):
    def __init__(self, obj):
        self.lock = threading.RLock()
        self._obj = obj

    def __enter__(self):
        self.lock.acquire()
        return self._obj

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.lock.release()


class HTTP20Connection(object):

    version = HTTPVersion.http20

    def __init__(self, host, port=None, secure=None, window_manager=None,
                 enable_push=False, ssl_context=None, proxy_host=None,
                 proxy_port=None, force_proto=None, proxy_headers=None,
                 timeout=None, **kwargs):
        """
        Creates an HTTP/2 connection to a specific server.
        """
        if port is None:
            self.host, self.port = to_host_port_tuple(host, default_port=443)
        else:
            self.host, self.port = host, port

        if secure is not None:
            self.secure = secure
        elif self.port == 443:
            self.secure = True
        else:
            self.secure = False

        self._enable_push = enable_push
        self.ssl_context = ssl_context

        if proxy_host and proxy_port is None:
            self.proxy_host, self.proxy_port = to_host_port_tuple(
                proxy_host, default_port=8080
            )
        elif proxy_host:
            self.proxy_host, self.proxy_port = proxy_host, proxy_port
        else:
            self.proxy_host = None
            self.proxy_port = None
        self.proxy_headers = proxy_headers

        self.network_buffer_size = 65536

        self.force_proto = force_proto

        self._lock = threading.RLock()

        self.__wm_class = window_manager or FlowControlManager
        self.__init_state()

        self._timeout = timeout

        return

    def __init_state(self):
        """
        Initializes the 'mutable state' portions of the HTTP/2 connection
        object.

        This method exists to enable HTTP20Connection objects to be reused if
        they're closed, by resetting the connection object to its basic state
        whenever it ends up closed. Any situation that needs to recreate the
        connection can call this method and it will be done.

        This is one of the only methods in hyper that is truly private, as
        users should be strongly discouraged from messing about with connection
        objects themselves.
        """
        self._conn = _LockedObject(h2.connection.H2Connection())

        self.streams = {}
        self.recent_stream = None
        self.next_stream_id = 1
        self.reset_streams = set()
        self.recent_recv_streams = set()

        self._sock = None

        self.window_manager = self.__wm_class(65535)

        return

    def ping(self, opaque_data):
        pass

    def request(self, method, url, body=None, headers=None):
        """
        This will send a request to the server using the HTTP request method
        ``method`` and the selector ``url``. If the ``body`` argument is
        present, it should be string or bytes object of data to send after the
        headers are finished. Strings are encoded as UTF-8. To use other
        encodings, pass a bytes object. The Content-Length header is set to the
        length of the body field.

        Concurrency
        -----------

        This method is thread-safe.

        :param method: The request method, e.g. ``'GET'``.
        :param url: The URL to contact, e.g. ``'/path/segment'``.
        :param body: (optional) The request body to send. Must be a bytestring
            or a file-like object.
        :param headers: (optional) The headers to send on the request.
        :returns: A stream ID for the request.
        """
        headers = headers or {}

        with self._lock:

            stream_id = self.putrequest(method, url)

            default_headers = (':method', ':scheme', ':authority', ':path')
            all_headers = headers.items()
            if self.proxy_host and not self.secure:
                proxy_headers = self.proxy_headers or {}
                all_headers = itertools.chain(all_headers,
                                              proxy_headers.items())
            for name, value in all_headers:
                is_default = to_native_string(name) in default_headers
                self.putheader(name, value, stream_id, replace=is_default)

            if body and isinstance(body, (unicode, bytes)):
                body = to_bytestring(body)

            self.endheaders(message_body=body, final=True, stream_id=stream_id)

            return stream_id

    def _get_stream(self, stream_id):
        if stream_id is None:
            return self.recent_stream
        elif stream_id in self.reset_streams or stream_id not in self.streams:
            raise StreamResetError("Stream forcefully closed")
        else:
            return self.streams[stream_id]

    def get_response(self, stream_id=None):
        """
        Should be called after a request is sent to get a response from the
        server. If sending multiple parallel requests, pass the stream ID of
        the request whose response you want. Returns a
        :class:`HTTP20Response <hyper.HTTP20Response>` instance.
        If you pass no ``stream_id``, you will receive the oldest
        :class:`HTTPResponse <hyper.HTTP20Response>` still outstanding.

        Concurrency
        -----------

        This method is thread-safe.

        :param stream_id: (optional) The stream ID of the request for which to
            get a response.
        :returns: A :class:`HTTP20Response <hyper.HTTP20Response>` object.
        """
        stream = self._get_stream(stream_id)
        return HTTP20Response(stream.getheaders(), stream)

    def get_pushes(self, stream_id=None, capture_all=False):
        pass

    def connect(self):
        """
        Connect to the server specified when the object was created. This is a
        no-op if we're already connected.

        Concurrency
        -----------

        This method is thread-safe. It may be called from multiple threads, and
        is a noop for all threads apart from the first.

        :returns: Nothing.

        """
        with self._lock:
            if self._sock is not None:
                return

            if isinstance(self._timeout, tuple):
                connect_timeout = self._timeout[0]
                read_timeout = self._timeout[1]
            else:
                connect_timeout = self._timeout
                read_timeout = self._timeout

            if self.proxy_host and self.secure:
                sock = _create_tunnel(
                    self.proxy_host,
                    self.proxy_port,
                    self.host,
                    self.port,
                    proxy_headers=self.proxy_headers,
                    timeout=self._timeout
                )
            elif self.proxy_host:
                sock = socket.create_connection(
                    (self.proxy_host, self.proxy_port),
                    timeout=connect_timeout
                )
            else:
                sock = socket.create_connection((self.host, self.port),
                                                timeout=connect_timeout)

            if self.secure:
                sock, proto = wrap_socket(sock, self.host, self.ssl_context,
                                          force_proto=self.force_proto)
            else:
                proto = H2C_PROTOCOL

            log.debug("Selected NPN protocol: %s", proto)
            assert proto in H2_NPN_PROTOCOLS or proto == H2C_PROTOCOL, (
                "No suitable protocol found. Supported protocols: %s. "
                "Check your OpenSSL version."
            ) % ','.join(H2_NPN_PROTOCOLS + [H2C_PROTOCOL])

            self._sock = BufferedSocket(sock, self.network_buffer_size)

            self._sock.settimeout(read_timeout)

            self._send_preamble()

    def _connect_upgrade(self, sock):
        """
        Called by the generic HTTP connection when we're being upgraded. Locks
        in a new socket and places the backing state machine into an upgrade
        state, then sends the preamble.
        """
        self._sock = sock

        with self._conn as conn:
            conn.initiate_upgrade_connection()
            conn.update_settings(
                {h2.settings.ENABLE_PUSH: int(self._enable_push)}
            )
        self._send_outstanding_data()

        s = self._new_stream(local_closed=True)
        self.recent_stream = s

        self._recv_cb()

    def _send_preamble(self):
        """
        Sends the necessary HTTP/2 preamble.
        """
        with self._conn as conn:
            conn.initiate_connection()
            conn.update_settings(
                {h2.settings.ENABLE_PUSH: int(self._enable_push)}
            )
        self._send_outstanding_data()

        self._recv_cb()

    def close(self, error_code=None):
        """
        Close the connection to the server.

        Concurrency
        -----------

        This method is thread-safe.

        :param error_code: (optional) The error code to reset all streams with.
        :returns: Nothing.
        """
        with self._lock:
            for stream in list(self.streams.values()):
                log.debug("Close stream %d" % stream.stream_id)
                stream.close(error_code)

            try:
                with self._conn as conn:
                    conn.close_connection(error_code or 0)
                self._send_outstanding_data(tolerate_peer_gone=True)
            except Exception as e:  # pragma: no cover
                log.warn("GoAway frame could not be sent: %s" % e)

            if self._sock is not None:
                self._sock.close()
            self.__init_state()

    def _send_outstanding_data(self, tolerate_peer_gone=False,
                               send_empty=True):
        with self._lock:
            with self._conn as conn:
                data = conn.data_to_send()
            if data or send_empty:
                self._send_cb(data, tolerate_peer_gone=tolerate_peer_gone)

    def putrequest(self, method, selector, **kwargs):
        """
        This should be the first call for sending a given HTTP request to a
        server. It returns a stream ID for the given connection that should be
        passed to all subsequent request building calls.

        Concurrency
        -----------

        This method is thread-safe. It can be called from multiple threads,
        and each thread should receive a unique stream ID.

        :param method: The request method, e.g. ``'GET'``.
        :param selector: The path selector.
        :returns: A stream ID for the request.
        """
        s = self._new_stream()

        s.add_header(":method", method)
        s.add_header(":scheme", "https" if self.secure else "http")
        s.add_header(":authority", self.host)
        s.add_header(":path", selector)

        self.recent_stream = s

        return s.stream_id

    def putheader(self, header, argument, stream_id=None, replace=False):
        """
        Sends an HTTP header to the server, with name ``header`` and value
        ``argument``.

        Unlike the ``httplib`` version of this function, this version does not
        actually send anything when called. Instead, it queues the headers up
        to be sent when you call
        :meth:`endheaders() <hyper.HTTP20Connection.endheaders>`.

        This method ensures that headers conform to the HTTP/2 specification.
        In particular, it strips out the ``Connection`` header, as that header
        is no longer valid in HTTP/2. This is to make it easy to write code
        that runs correctly in both HTTP/1.1 and HTTP/2.

        :param header: The name of the header.
        :param argument: The value of the header.
        :param stream_id: (optional) The stream ID of the request to add the
            header to.
        :returns: Nothing.
        """
        stream = self._get_stream(stream_id)
        stream.add_header(header, argument, replace)

        return

    def endheaders(self, message_body=None, final=False, stream_id=None):
        """
        Sends the prepared headers to the server. If the ``message_body``
        argument is provided it will also be sent to the server as the body of
        the request, and the stream will immediately be closed. If the
        ``final`` argument is set to True, the stream will also immediately
        be closed: otherwise, the stream will be left open and subsequent calls
        to ``send()`` will be required.

        :param message_body: (optional) The body to send. May not be provided
            assuming that ``send()`` will be called.
        :param final: (optional) If the ``message_body`` parameter is provided,
            should be set to ``True`` if no further data will be provided via
            calls to :meth:`send() <hyper.HTTP20Connection.send>`.
        :param stream_id: (optional) The stream ID of the request to finish
            sending the headers on.
        :returns: Nothing.
        """
        self.connect()

        stream = self._get_stream(stream_id)

        headers_only = (message_body is None and final)

        with self._lock:
            stream.send_headers(headers_only)

            if message_body is not None:
                stream.send_data(message_body, final)

            self._send_outstanding_data()

        return

    def send(self, data, final=False, stream_id=None):
        """
        Sends some data to the server. This data will be sent immediately
        (excluding the normal HTTP/2 flow control rules). If this is the last
        data that will be sent as part of this request, the ``final`` argument
        should be set to ``True``. This will cause the stream to be closed.

        :param data: The data to send.
        :param final: (optional) Whether this is the last bit of data to be
            sent on this request.
        :param stream_id: (optional) The stream ID of the request to send the
            data on.
        :returns: Nothing.
        """
        stream = self._get_stream(stream_id)
        stream.send_data(data, final)

        return

    def _new_stream(self, stream_id=None, local_closed=False):
        """
        Returns a new stream object for this connection.
        """
        with self._lock:
            s = Stream(
                stream_id or self.next_stream_id,
                self.__wm_class(DEFAULT_WINDOW_SIZE),
                self._conn,
                self._send_outstanding_data,
                self._recv_cb,
                self._stream_close_cb,
            )
            s.local_closed = local_closed
            self.streams[s.stream_id] = s
            self.next_stream_id += 2

            return s

    def _send_cb(self, data, tolerate_peer_gone=False):
        """
        This is the callback used by streams to send data on the connection.

        This acts as a dumb wrapper around the socket send method.
        """
        with self._lock:
            try:
                self._sock.sendall(data)
            except socket.error as e:
                if (not tolerate_peer_gone or
                        e.errno not in (errno.EPIPE, errno.ECONNRESET)):
                    raise

    def _adjust_receive_window(self, frame_len):
        """
        Adjusts the window size in response to receiving a DATA frame of length
        ``frame_len``. May send a WINDOWUPDATE frame if necessary.
        """
        with self._lock:
            increment = self.window_manager._handle_frame(frame_len)

            if increment:
                with self._conn as conn:
                    conn.increment_flow_control_window(increment)
                self._send_outstanding_data(tolerate_peer_gone=True)

        return

    def _single_read(self):
        """
        Performs a single read from the socket and hands the data off to the
        h2 connection object.
        """
        with self._lock:
            if self._sock is None:
                raise ConnectionError('tried to read after connection close')
            self._sock.fill()
            data = self._sock.buffer.tobytes()
            self._sock.advance_buffer(len(data))
            with self._conn as conn:
                events = conn.receive_data(data)
            stream_ids = set(getattr(e, 'stream_id', -1) for e in events)
            stream_ids.discard(-1)  # sentinel
            stream_ids.discard(0)  # connection events
            self.recent_recv_streams |= stream_ids

        for event in events:
            if isinstance(event, h2.events.DataReceived):
                self._adjust_receive_window(event.flow_controlled_length)
                self.streams[event.stream_id].receive_data(event)
            elif isinstance(event, h2.events.PushedStreamReceived):
                if self._enable_push:
                    self._new_stream(event.pushed_stream_id, local_closed=True)
                    self.streams[event.parent_stream_id].receive_push(event)
                else:
                    self._send_rst_frame(event.pushed_stream_id, 7)
            elif isinstance(event, h2.events.ResponseReceived):
                self.streams[event.stream_id].receive_response(event)
            elif isinstance(event, h2.events.TrailersReceived):
                self.streams[event.stream_id].receive_trailers(event)
            elif isinstance(event, h2.events.StreamEnded):
                self.streams[event.stream_id].receive_end_stream(event)
            elif isinstance(event, h2.events.StreamReset):
                if event.stream_id not in self.reset_streams:
                    self.reset_streams.add(event.stream_id)
                    self.streams[event.stream_id].receive_reset(event)
            elif isinstance(event, h2.events.ConnectionTerminated):
                self.close()

                if event.error_code != 0:
                    try:
                        name, number, description = errors.get_data(
                            event.error_code
                        )
                    except ValueError:
                        error_string = (
                            "Encountered error code %d" % event.error_code
                        )
                    else:
                        error_string = (
                            "Encountered error %s %s: %s" %
                            (name, number, description)
                        )

                    raise ConnectionError(error_string)
            else:
                log.info("Received unhandled event %s", event)

        self._send_outstanding_data(tolerate_peer_gone=True, send_empty=False)

    def _recv_cb(self, stream_id=0):
        """
        This is the callback used by streams to read data from the connection.

        This stream reads what data it can, and throws it into the underlying
        connection, before farming out any events that fire to the relevant
        streams. If the socket remains readable, it will then optimistically
        continue to attempt to read.

        This is generally called by a stream, not by the connection itself, and
        it's likely that streams will read a frame that doesn't belong to them.

        :param stream_id: (optional) The stream ID of the stream reading data
            from the connection.

        """
        with self._lock:
            log.debug('recv for stream %d with %s already present',
                      stream_id,
                      self.recent_recv_streams)
            if stream_id in self.recent_recv_streams:
                self.recent_recv_streams.discard(stream_id)
                return

            if stream_id:
                self._get_stream(stream_id)

            self._single_read()
            count = 9
            retry_wait = 0.05  # can improve responsiveness to delay the retry

            while count and self._sock is not None and self._sock.can_read:
                try:
                    self._single_read()
                except ConnectionResetError:
                    break
                except ssl.SSLError as e:  # pragma: no cover
                    if e.args[0] in TRANSIENT_SSL_ERRORS:
                        continue
                    else:
                        raise
                except socket.error as e:  # pragma: no cover
                    if e.errno in (errno.EINTR, errno.EAGAIN):
                        time.sleep(retry_wait)
                        continue
                    elif e.errno == errno.ECONNRESET:
                        break
                    else:
                        raise

                count -= 1

    def _send_rst_frame(self, stream_id, error_code):
        """
        Send reset stream frame with error code and remove stream from map.
        """
        with self._lock:
            with self._conn as conn:
                conn.reset_stream(stream_id, error_code=error_code)
            self._send_outstanding_data()

        with self._lock:
            try:
                del self.streams[stream_id]
                self.recent_recv_streams.discard(stream_id)
            except KeyError as e:  # pragma: no cover
                log.warn(
                    "Stream with id %d does not exist: %s",
                    stream_id, e)

            self.reset_streams.add(stream_id)

    def _stream_close_cb(self, stream_id):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()
        return False  # Never swallow exceptions.
