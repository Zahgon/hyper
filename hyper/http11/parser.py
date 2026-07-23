# -*- coding: utf-8 -*-
from collections import namedtuple


Response = namedtuple(
    'Response', ['status', 'msg', 'minor_version', 'headers', 'consumed']
)


class ParseError(Exception):
    pass


class Parser(object):
    def __init__(self):
        pass

    def parse_response(self, buffer):
        """
        Parses a single HTTP response from a buffer.
        :param buffer: A ``memoryview`` object wrapping a buffer containing a
            HTTP response.
        :returns: A :class:`Response <hyper.http11.parser.Response>` object, or
            ``None`` if there is not enough data in the buffer.
        """
        temp_buffer = buffer.tobytes()

        index = temp_buffer.find(b'\n')
        if index == -1:
            return None

        version, status, reason = (
                temp_buffer[0:index].split(None, 2) + [b''])[:3]
        if not version.startswith(b'HTTP/1.'):
            raise ParseError("Not HTTP/1.X!")

        minor_version = int(version[7:])
        status = int(status)
        reason = memoryview(reason.strip())

        index += 1

        end_index = index
        headers = []

        while True:
            end_index = temp_buffer.find(b'\n', index)
            if end_index == -1:
                return None
            elif (end_index - index) <= 1:
                end_index += 1
                break

            name, value = temp_buffer[index:end_index].split(b':', 1)
            value = value.strip()
            headers.append((memoryview(name), memoryview(value)))
            index = end_index + 1

        resp = Response(status, reason, minor_version, headers, end_index)
        return resp
