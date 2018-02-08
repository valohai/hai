DICT_NAME = '_event_emitter_dict'


def _get_event_emitter_dict(obj):
    return obj.__dict__.setdefault(DICT_NAME, {})


class EventEmitter:
    event_types = set()

    def on(self, event, handler):
        if event != '*' and event not in self.event_types:
            raise ValueError('event type {} is not known'.format(event))

        _get_event_emitter_dict(self).setdefault(event, set()).add(handler)

    def off(self, event, handler):
        _get_event_emitter_dict(self).get(event, set()).discard(handler)

    def emit(self, event, args=None, quiet=True):
        if event not in self.event_types:
            raise ValueError('event type {} is not known'.format(event))
        emitter_dict = _get_event_emitter_dict(self)
        handlers = (
            emitter_dict.get(event, set()) |
            emitter_dict.get('*', set())
        )
        args = (args or {})
        args.setdefault('sender', self)
        args.setdefault('event', event)
        for handler in handlers:
            try:
                handler(**args)
            except:  # noqa
                if not quiet:
                    raise
