from typing import Any, Callable, Dict, Optional

DICT_NAME = '_event_emitter_dict'

Handler = Callable[..., Any]


def _get_event_emitter_dict(obj: Any) -> Dict[str, Any]:
    return obj.__dict__.setdefault(DICT_NAME, {})  # type: ignore[no-any-return]


class EventEmitter:
    event_types = set()  # type: set[str]

    def on(self, event: str, handler: Handler) -> None:
        if event != '*' and event not in self.event_types:
            raise ValueError(f'event type {event} is not known')

        _get_event_emitter_dict(self).setdefault(event, set()).add(handler)

    def off(self, event: str, handler: Handler) -> None:
        _get_event_emitter_dict(self).get(event, set()).discard(handler)

    def emit(self, event: str, args: Optional[Dict[str, Any]] = None, quiet: bool = True) -> None:
        if event not in self.event_types:
            raise ValueError(f'event type {event} is not known')
        emitter_dict = _get_event_emitter_dict(self)
        handlers = (
            emitter_dict.get(event, set()) | emitter_dict.get('*', set())
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
