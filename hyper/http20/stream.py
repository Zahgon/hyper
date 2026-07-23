# -*- coding: utf-8 -*-
import h2.exceptions

from ..common.headers import HTTPHeaderMap
from .util import h2_safe_headers
import logging

log = logging.getLogger(__name__)

MAX_CHUNK = 1024


class Stream(object):
    def __init__(self,
                 stream_id,
                 window_manager,
                 connection,
                 send_outstanding_data,
                 recv_cb,
                 close_cb):
        self.stream_id = stream_id
        self.headers = HTTPHeaderMap()

        self.response_headers = None

        self.response_trailers = None

        self.promised_headers = {}

        self.data = []

        self.remote_closed = False

        self.local_closed = False

        self._in_window_manager = window_manager

        self._conn = connection

        self._send_outstanding_data = send_outstanding_data
        self._recv_cb = recv_cb
        self._close_cb = close_cb

    def add_header(self, name, value, replace=False):
        """
        Adds a single HTTP header to the headers to be sent on the request.
        """
        if not replace:
            self.headers[name] = value
        else:
            self.headers.replace(name, value)

    def send_headers(self, end_stream=False):
        """
        Sends the complete saved header block on the stream.
        """
        headers = self.get_headers()
        with self._conn as conn:
            conn.send_headers(self.stream_id, headers, end_stream)
        self._send_outstanding_data()

        if end_stream:
            self.local_closed = True

    def send_data(self, data, final):
        """
        Send some data on the stream. If this is the end of the data to be
        sent, the ``final`` flag _must_ be set to True. If no data is to be
        sent, set ``data`` to ``None``.
        """
        def file_iterator(fobj):
            while True:
                data = fobj.read(MAX_CHUNK)
                yield data
                if len(data) < MAX_CHUNK:
                    break

        if hasattr(data, 'read'):
            chunks = file_iterator(data)
        else:
            chunks = (data[i:i+MAX_CHUNK]
                      for i in range(0, len(data), MAX_CHUNK))

        cur_chunk = None
        try:
            cur_chunk = next(chunks)
            while True:
                next_chunk = next(chunks)
                self._send_chunk(cur_chunk, False)
                cur_chunk = next_chunk
        except StopIteration:
            if cur_chunk is not None:  # cur_chunk none when no chunks to send
                self._send_chunk(cur_chunk, final)

    def _read(self, amt=None):
        """
        Read data from the stream. Unlike a normal read behaviour, this
        function returns _at least_ ``amt`` data, but may return more.
        """
        def listlen(list):
            return sum(map(len, list))

        while (not self.remote_closed and
                (amt is None or listlen(self.data) < amt)):
            self._recv_cb(stream_id=self.stream_id)

        result = b''.join(self.data)
        self.data = []
        return result

    def _read_one_frame(self):
        """
        Reads a single data frame from the stream and returns it.
        """
        while not self.remote_closed and not self.data:
            self._recv_cb(stream_id=self.stream_id)

        try:
            return self.data.pop(0)
        except IndexError:
            return None

    def receive_response(self, event):
        """
        Receive response headers.
        """
        self.response_headers = HTTPHeaderMap(event.headers)

    def receive_trailers(self, event):
        """
        Receive response trailers.
        """
        self.response_trailers = HTTPHeaderMap(event.headers)

    def receive_push(self, event):
        """
        Receive the request headers for a pushed stream.
        """
        self.promised_headers[event.pushed_stream_id] = event.headers

    def receive_data(self, event):
        """
        Receive a chunk of data.
        """
        size = event.flow_controlled_length
        increment = self._in_window_manager._handle_frame(size)

        self.data.append(event.data)

        if increment:
            try:
                with self._conn as conn:
                    conn.increment_flow_control_window(
                        increment, stream_id=self.stream_id
                    )
            except h2.exceptions.StreamClosedError:
                pass
            else:
                self._send_outstanding_data()

    def receive_end_stream(self, event):
        """
        All of the data is returned now.
        """
        self.remote_closed = True

    def receive_reset(self, event):
        """
        Stream forcefully reset.
        """
        self.remote_closed = True
        self._close_cb(self.stream_id)

    def get_headers(self):
        """
        Provides the headers to the connection object.
        """
        return h2_safe_headers(self.headers)

    def getheaders(self):
        """
        Once all data has been sent on this connection, returns a key-value set
        of the headers of the response to the original request.
        """
        while self.response_headers is None:
            self._recv_cb(stream_id=self.stream_id)

        self._in_window_manager.document_size = (
            int(self.response_headers.get(b'content-length', [0])[0])
        )

        return self.response_headers

    def gettrailers(self):
        pass

    def get_pushes(self, capture_all=False):
        pass

    def close(self, error_code=None):
        """
        Closes the stream. If the stream is currently open, attempts to close
        it as gracefully as possible.

        :param error_code: (optional) The error code to reset the stream with.
        :returns: Nothing.
        """
        if not (self.remote_closed and self.local_closed):
            try:
                with self._conn as conn:
                    conn.reset_stream(self.stream_id, error_code or 0)
            except h2.exceptions.ProtocolError:
                pass
            else:
                self._send_outstanding_data(tolerate_peer_gone=True)
            self.remote_closed = True
            self.local_closed = True

        self._close_cb(self.stream_id)

    @property
    def _out_flow_control_window(self):
        pass

    def _send_chunk(self, data, final):
        """
        Implements most of the sending logic.

        Takes a single chunk of size at most MAX_CHUNK, wraps it in a frame and
        sends it. Optionally sets the END_STREAM flag if this is the last chunk
        (determined by being of size less than MAX_CHUNK) and no more data is
        to be sent.
        """
        while len(data) > self._out_flow_control_window:
            self._recv_cb()

        with self._conn as conn:
            conn.send_data(
                stream_id=self.stream_id, data=data, end_stream=final
            )
        self._send_outstanding_data()

        if final:
            self.local_closed = True
