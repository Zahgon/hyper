# -*- coding: utf-8 -*-
import logging
import zlib
import brotli

from ..common.decoder import DeflateDecoder
from ..common.headers import HTTPHeaderMap
from ..common.util import HTTPVersion

log = logging.getLogger(__name__)


def strip_headers(headers):
    pass


decompressors = {
    b'gzip': lambda: zlib.decompressobj(16 + zlib.MAX_WBITS),
    b'br': brotli.Decompressor,
    b'deflate': DeflateDecoder
}


class HTTP20Response(object):

    version = HTTPVersion.http20
    _decompressobj = None

    def __init__(self, headers, stream):
        self.reason = ''

        status = headers[b':status'][0]
        strip_headers(headers)

        self.status = int(status)

        self.headers = headers

        self._trailers = None

        self._stream = stream

        self._data_buffer = b''

        for c in self.headers.get(b'content-encoding', []):
            if c in decompressors:
                self._decompressobj = decompressors.get(c)()
                break

    @property
    def trailers(self):
        pass

    def read(self, amt=None, decode_content=True):
        """
        Reads the response body, or up to the next ``amt`` bytes.

        :param amt: (optional) The amount of data to read. If not provided, all
            the data will be read from the response.
        :param decode_content: (optional) If ``True``, will transparently
            decode the response data.
        :returns: The read data. Note that if ``decode_content`` is set to
            ``True``, the actual amount of data returned may be different to
            the amount requested.
        """
        if amt is not None and amt <= len(self._data_buffer):
            data = self._data_buffer[:amt]
            self._data_buffer = self._data_buffer[amt:]
            response_complete = False
        elif amt is not None:
            read_amt = amt - len(self._data_buffer)
            self._data_buffer += self._stream._read(read_amt)
            data = self._data_buffer[:amt]
            self._data_buffer = self._data_buffer[amt:]
            response_complete = len(data) < amt
        else:
            data = b''.join([self._data_buffer, self._stream._read()])
            response_complete = True

        if decode_content and self._decompressobj and data:
            data = self._decompressobj.decompress(data)

        if response_complete:
            if decode_content and self._decompressobj:
                data += self._decompressobj.flush()

            if self._stream.response_headers:
                self.headers.merge(self._stream.response_headers)

        if response_complete:
            self.close()

        return data

    def read_chunked(self, decode_content=True):
        """
        Reads chunked transfer encoded bodies. This method returns a generator:
        each iteration of which yields one data frame *unless* the frames
        contain compressed data and ``decode_content`` is ``True``, in which
        case it yields whatever the decompressor provides for each chunk.

        .. warning:: This may yield the empty string, without that being the
                     end of the body!
        """
        while True:
            data = self._stream._read_one_frame()

            if data is None:
                break

            if decode_content and self._decompressobj:
                data = self._decompressobj.decompress(data)

            yield data

        if decode_content and self._decompressobj:
            yield self._decompressobj.flush()

        self.close()

        return

    def fileno(self):
        """
        Return the ``fileno`` of the underlying socket. This function is
        currently not implemented.
        """
        raise NotImplementedError("Not currently implemented.")

    def close(self):
        """
        Close the response. In effect this closes the backing HTTP/2 stream.

        :returns: Nothing.
        """
        self._stream.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False  # Never swallow exceptions.


class HTTP20Push(object):
    def __init__(self, request_headers, stream):
        self.scheme = request_headers[b':scheme'][0]
        self.method = request_headers[b':method'][0]
        self.authority = request_headers[b':authority'][0]
        self.path = request_headers[b':path'][0]

        strip_headers(request_headers)

        self.request_headers = request_headers

        self._stream = stream

    def get_response(self):
        """
        Get the pushed response provided by the server.

        :returns: A :class:`HTTP20Response <hyper.HTTP20Response>` object
            representing the pushed response.
        """
        return HTTP20Response(self._stream.getheaders(), self._stream)

    def cancel(self):
        pass
