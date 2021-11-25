import contextlib
import subprocess

from hai.pipe_pump import ChunkPipePump, LinePipePump


def test_line_pipe_pump():
    proc = subprocess.Popen(
        args="echo hello; sleep 0.5; echo world",
        stdout=subprocess.PIPE,
        shell=True,
        bufsize=0,
    )

    line_lists = []

    def add_handler(key, lines):
        line_lists.append(lines[:])
        lines[:] = [l[::-1] for l in lines]  # See that we can mutate the list

    with contextlib.closing(LinePipePump()) as pp:
        pp.add_line_handler(add_handler)
        pp.register('stdout', proc.stdout)
        pp.as_thread().start()
        proc.wait()

    assert line_lists == [
        [b'hello'],
        [b'olleh', b'world'],  # olleh due to mutation
    ]

    assert pp.get_value('stdout') == b'hello\ndlrow'  # and again re-reversed for the final list


def test_chunk_pipe_pump():
    proc = subprocess.Popen(
        args='dd if=/dev/urandom bs=100 count=10',
        stdout=subprocess.PIPE,
        shell=True,
        bufsize=0,
    )

    chunks = []

    def add_handler(key, chunk):
        chunks.append(chunk)

    with contextlib.closing(ChunkPipePump()) as pp:
        pp.add_chunk_handler(add_handler)
        pp.register('stdout', proc.stdout)
        while proc.poll() is None:  # demonstrate hand-pumping
            pp.pump(0.05)

    chunk_lengths = [len(chunk) for chunk in chunks]
    assert chunk_lengths == [256, 256, 256, 232]
    assert sum(chunk_lengths) == 1000
