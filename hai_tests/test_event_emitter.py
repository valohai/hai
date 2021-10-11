import pytest

from hai.event_emitter import EventEmitter


class Thing(EventEmitter):
    event_types = {'one', 'two'}


@pytest.mark.parametrize('omni', (False, True))
def test_event_emitter(omni):
    t = Thing()
    events = []

    def handle(sender, **args):
        assert sender is t
        events.append(args)

    if omni:
        t.on('*', handle)
    else:
        t.on('one', handle)
    t.emit('one')
    t.emit('two')
    t.off('one', handle)
    t.emit('one', {'oh': 'no'})

    if omni:
        assert events == [
            {'event': 'one'},
            {'event': 'two'},
            {'event': 'one', 'oh': 'no'},
        ]
    else:
        assert events == [
            {'event': 'one'},
        ]


def test_event_emitter_exceptions():
    t = Thing()

    def handle(**args):
        raise OSError('oh no')

    t.on('*', handle)
    t.emit('one')
    with pytest.raises(IOError):
        t.emit('one', quiet=False)


def test_event_emitter_unknown_event_types():
    t = Thing()

    with pytest.raises(ValueError):
        t.on('hullo', None)

    with pytest.raises(ValueError):
        t.emit('hello')
