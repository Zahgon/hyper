# -*- coding: utf-8 -*-


class BaseFlowControlManager(object):
    def __init__(self, initial_window_size, document_size=None):
        self.initial_window_size = initial_window_size

        self.window_size = initial_window_size

        self.document_size = document_size

    def increase_window_size(self, frame_size):
        """
        Determine whether or not to emit a WINDOWUPDATE frame.

        This method should be overridden to determine, based on the state of
        the system and the size of the received frame, whether or not a
        WindowUpdate frame should be sent for the stream.

        This method should *not* adjust any of the member variables of this
        class.

        Note that this method is called before the window size is decremented
        as a result of the frame being handled.

        :param frame_size: The size of the received frame. Note that this *may*
          be zero. When this parameter is zero, it's possible that a
          WINDOWUPDATE frame may want to be emitted anyway. A zero-length frame
          size is usually associated with a change in the size of the receive
          window due to a SETTINGS frame.
        :returns: The amount to increase the receive window by. Return zero if
          the window should not be increased.
        """
        raise NotImplementedError(
            "FlowControlManager is an abstract base class"
        )

    def blocked(self):
        """
        Called whenever the remote endpoint reports that it is blocked behind
        the flow control window.

        When this method is called the remote endpoint is signaling that it
        has more data to send and that the transport layer is capable of
        transmitting it, but that the HTTP/2 flow control window prevents it
        being sent.

        This method should return the size by which the window should be
        incremented, which may be zero. This method should *not* adjust any
        of the member variables of this class.

        :returns: The amount to increase the receive window by. Return zero if
          the window should not be increased.
        """
        raise NotImplementedError(
            "FlowControlManager is an abstract base class"
        )

    def _handle_frame(self, frame_size):
        """
        This internal method is called by the connection or stream that owns
        the flow control manager. It handles the generic behaviour of flow
        control managers: namely, keeping track of the window size.
        """
        rc = self.increase_window_size(frame_size)
        self.window_size -= frame_size
        self.window_size += rc
        return rc

    def _blocked(self):
        pass


class FlowControlManager(BaseFlowControlManager):
    def increase_window_size(self, frame_size):
        future_window_size = self.window_size - frame_size

        if ((future_window_size < (self.initial_window_size / 4)) or
                (future_window_size < 1000)):
            return self.initial_window_size - future_window_size

        return 0

    def blocked(self):
        pass
