import re
import selectors
import threading


class BasePipePump:
    """
    Pump file objects into buffers.
    """
    read_size = 1024

    def __init__(self):
        self.selector = selectors.DefaultSelector()
        self.buffers = {}
        self.selector_lock = threading.Lock()

    def register(self, key, fileobj):
        """
        Register a file object to be polled by `pump`.

        :param key: Queue key string.
        :param fileobj: File object to poll.
        """
        key = str(key)
        self.buffers[key] = b''
        if fileobj:
            self.selector.register(fileobj, selectors.EVENT_READ, data=key)

    def pump(self, timeout=0):
        """
        Pump the pipes and buffer incoming data.

        :param timeout: Timeout for reading.
                        0 = just poll and return immediately.
        :type timeout: float
        """
        with self.selector_lock:
            if not self.selector:  # pragma: no cover
                return
            for (key, event) in self.selector.select(timeout=timeout):
                data = key.fileobj.read(self.read_size)
                self.feed(key.data, data)

    def feed(self, key, data):
        """
        Add data to the keyed buffer.

        This calls `_process_buffer` to process the newly augmented buffer,
        which may call subclass callbacks.

        :param key: Queue key string.
        :param data: Byte data to buffer.
        """
        buffered_data = self.buffers[key]
        if data:
            buffered_data += data
            buffered_data = self._process_buffer(key, buffered_data)
        self.buffers[key] = buffered_data

    def _process_buffer(self, key, buffer):  # pragma: no cover
        """
        Internal; process a given buffer somehow and return what should be left
        in the internal buffer.

        See `LinePipePump` for a concrete idea on how to use this.

        :param key:
        :param buffer:
        :return:
        """
        return buffer

    def close(self):
        self.close_selector()

    def close_selector(self):
        if self.selector:  # pragma: no branch
            with self.selector_lock:
                self.selector.close()
            self.selector = None

    def as_thread(self, interval=0.05):
        """
        Return an unstarted threading.Thread object for pumping data.

        The thread will die when the PipePump is `close`d.

        :param interval: Poll interval.
        :type interval: float
        :return: Thread
        :rtype: threading.Thread
        """

        def pumper():
            while self.selector is not None:
                self.pump(timeout=interval)

        return threading.Thread(target=pumper, name='Thread for %r' % self)


class LinePipePump(BasePipePump):
    """
    A PipePump that processes the read data into items
    separated by a given bytestring.
    """

    def __init__(self, separator=b'\n'):
        """
        :param separator: Line separator byte sequence.
        """
        super(LinePipePump, self).__init__()
        assert isinstance(separator, bytes)
        self.separator = separator
        self.lines = {}
        self._line_handlers = []

    def add_line_handler(self, handler):
        """
        :param handler: Callable to call when a line is received; receives
                        the queue key and the queue of lines.  The callable
                        may mutate the queue at will; however this will
                        affect `get_value`.
        """
        assert callable(handler)
        self._line_handlers.append(handler)

    def _process_buffer(self, key, buffer):
        while self.separator in buffer:
            line, _, buffer = buffer.partition(self.separator)
            self.add_line(key, line)
        return buffer

    def add_line(self, key, line):
        """
        Add a line to the given queue.

        This will call the `on_line` callback, if one exists.

        :param key: Queue key.
        :param line: Line to add; preferably a bytestring.
        """
        key = str(key)
        if not isinstance(line, bytes):
            line = line.encode('utf-8')
        line_list = self.lines.setdefault(key, [])
        line_list.append(line)

        for handler in self._line_handlers:  # pragma: no branch
            handler(key, line_list)

    def get_value(self, key):
        """
        Get the full value of a line queue by the key `key`.

        :param key: Line queue key
        :return: bytestring of content
        """
        return self.separator.join(self.lines.get(str(key), ()))

    def close(self):
        # Flush all buffers when closing; any unfinished lines will thus
        # end up being posted as lines.
        self.pump()  # One more pump before closing!
        super(LinePipePump, self).close()
        for key, buffer in self.buffers.items():
            if buffer:  # pragma: no branch
                self.add_line(key, buffer)
        self.buffers.clear()


class ChunkPipePump(BasePipePump):
    """
    A PipePump that hands off read data in chunks of N bytes, then discards it.
    """

    def __init__(self, chunk_size=256):
        """
        :param chunk_size: Chunk size in bytes.
                           The final chunk might be shorter than this.
        :type chunk_size: int
        """
        super(ChunkPipePump, self).__init__()
        assert chunk_size > 0
        self.chunk_size = chunk_size
        self._chunk_handlers = []

    def add_chunk_handler(self, handler):
        """
        :param handler: Callable to call when a chunk is received; receives
                        the queue key and the received chunk.
        """
        assert callable(handler)
        self._chunk_handlers.append(handler)

    def _process_buffer(self, key, buffer):
        while len(buffer) >= self.chunk_size:
            chunk, buffer = buffer[:self.chunk_size], buffer[self.chunk_size:]
            self._handle_chunk(key, chunk)
        return buffer

    def _handle_chunk(self, key, chunk):
        for handler in self._chunk_handlers:  # pragma: no branch
            handler(key, chunk)

    def close(self):
        self.pump()  # One more pump before closing!
        super(ChunkPipePump, self).close()
        for key, buffer in self.buffers.items():
            if buffer:  # pragma: no branch
                self._handle_chunk(key, buffer)
        self.buffers.clear()


class CRLFPipePump(BasePipePump):
    """
    A pipe pump that knows how to handle carriage returns like they are handled on terminals.

    Unlike LinePipePump, this does not buffer any history in its own state, only the last line.
    """

    CRLF_SEP_RE = re.compile(br'^(.*?)([\r\n])')

    def __init__(self):
        super().__init__()
        self.line_state = {}
        self._handlers = []

    def add_handler(self, handler):
        """
        :param handler: Callable to call when a line is received; receives:

                        * the queue key
                        * the last line bytes, if any
                        * the new line bytes, if any
                        * and a flag describing
                          whether the new bytes should replace the latest line,
                          or add a new one to the buffer.

        :type handler: Callable[Tuple[str, Optional[bytes], bytes, bool], None]
        """
        assert callable(handler)
        self._handlers.append(handler)

    def _process_buffer(self, key, buffer):
        while True:
            m = self.CRLF_SEP_RE.match(buffer)
            if not m:
                break
            self._process_line(key, m.group(1), is_replace=(m.group(2) == b'\r'))
            buffer = buffer[m.end():]
        return buffer

    def _process_line(self, key, new_content, is_replace):
        if key in self.line_state:
            old_content, last_was_replace = self.line_state[key]
        else:
            old_content, last_was_replace = (None, False)
        self.line_state[key] = (new_content, is_replace)

        for handler in self._handlers:  # pragma: no branch
            handler(key, old_content, new_content, last_was_replace)

    def close(self):
        self.pump()  # One more pump before closing!
        super().close()
        for key, buffer in self.buffers.items():
            if buffer:  # pragma: no branch
                self._process_line(key, buffer, False)
        self.buffers.clear()
