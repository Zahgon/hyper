# -*- coding: utf-8 -*-
import select
from .exceptions import ConnectionResetError, LineTooLongError


class BufferedSocket(object):
    def __init__(self, sck, buffer_size=1000):
        """
        Create the buffered socket.

        :param sck: The socket to wrap.
        :param buffer_size: The size of the backing buffer in bytes. This
            parameter should be set to an appropriate value for your use case.
            Small values of ``buffer_size`` increase the overhead of buffer
            management: large values cause more memory to be used.
        """
        self._sck = sck

        self._backing_buffer = bytearray(buffer_size)
        self._buffer_view = memoryview(self._backing_buffer)

        self._buffer_size = buffer_size

        self._index = 0

        self._bytes_in_buffer = 0

    @property
    def _remaining_capacity(self):
        pass

    @property
    def _buffer_end(self):
        pass

    @property
    def can_read(self):
        pass

    @property
    def buffer(self):
        pass

    def advance_buffer(self, count):
        """
        Advances the buffer by the amount of data consumed outside the socket.
        """
        self._index += count
        self._bytes_in_buffer -= count

    def new_buffer(self):
        """
        This method moves all the data in the backing buffer to the start of
        a new, fresh buffer. This gives the ability to read much more data.
        """
        def read_all_from_buffer():
            end = self._index + self._bytes_in_buffer
            return self._buffer_view[self._index:end]

        new_buffer = bytearray(self._buffer_size)
        new_buffer_view = memoryview(new_buffer)
        new_buffer_view[0:self._bytes_in_buffer] = read_all_from_buffer()

        self._index = 0
        self._backing_buffer = new_buffer
        self._buffer_view = new_buffer_view

        return

    def recv(self, amt):
        """
        Read some data from the socket.

        :param amt: The amount of data to read.
        :returns: A ``memoryview`` object containing the appropriate number of
            bytes. The data *must* be copied out by the caller before the next
            call to this function.
        """
        if amt > self._buffer_size:
            amt = self._buffer_size

        if amt > self._remaining_capacity:
            self.new_buffer()

        if self._bytes_in_buffer >= amt:
            should_read = select.select([self._sck], [], [], 0)[0]
        else:
            should_read = True

        if should_read:
            count = self._sck.recv_into(self._buffer_view[self._buffer_end:])

            if not count and amt > self._bytes_in_buffer:
                raise ConnectionResetError()
            self._bytes_in_buffer += count

        amt = min(amt, self._bytes_in_buffer)
        data = self._buffer_view[self._index:self._index+amt]

        self._index += amt
        self._bytes_in_buffer -= amt

        return data

    def fill(self):
        """
        Attempts to fill the buffer as much as possible. It will block for at
        most the time required to have *one* ``recv_into`` call return.
        """
        if not self._remaining_capacity:
            self.new_buffer()

        count = self._sck.recv_into(self._buffer_view[self._buffer_end:])
        if not count:
            raise ConnectionResetError()

        self._bytes_in_buffer += count

        return

    def readline(self):
        """
        Read up to a newline from the network and returns it. The implicit
        maximum line length is the buffer size of the buffered socket.

        Note that, unlike recv, this method absolutely *does* block until it
        can read the line.

        :returns: A ``memoryview`` object containing the appropriate number of
            bytes. The data *must* be copied out by the caller before the next
            call to this function.
        """
        index = self._backing_buffer.find(
            b'\n',
            self._index,
            self._index + self._bytes_in_buffer
        )

        if index != -1:
            length = index + 1 - self._index
            data = self._buffer_view[self._index:self._index+length]
            self._index += length
            self._bytes_in_buffer -= length
            return data

        if self._index != 0:
            self.new_buffer()

        while self._bytes_in_buffer < self._buffer_size:
            count = self._sck.recv_into(self._buffer_view[self._buffer_end:])
            if not count:
                raise ConnectionResetError()

            first_new_byte = self._buffer_end
            self._bytes_in_buffer += count
            index = self._backing_buffer.find(
                b'\n',
                first_new_byte,
                first_new_byte + count,
            )

            if index != -1:
                assert not self._index
                length = index + 1
                data = self._buffer_view[:length]
                self._index += length
                self._bytes_in_buffer -= length
                return data

        raise LineTooLongError()

    def __getattr__(self, name):
        return getattr(self._sck, name)
