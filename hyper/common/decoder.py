# -*- coding: utf-8 -*-
import zlib


class DeflateDecoder(object):
    def __init__(self):
        self._first_try = True
        self._data = b''
        self._obj = zlib.decompressobj(zlib.MAX_WBITS)

    def __getattr__(self, name):
        return getattr(self._obj, name)

    def decompress(self, data):
        if not self._first_try:
            return self._obj.decompress(data)

        self._data += data
        try:
            return self._obj.decompress(data)
        except zlib.error:
            self._first_try = False
            self._obj = zlib.decompressobj(-zlib.MAX_WBITS)
            try:
                return self.decompress(self._data)
            finally:
                self._data = None
