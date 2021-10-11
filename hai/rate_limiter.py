import time
from enum import Enum
from typing import Dict, Optional, Union


class StateChange(Enum):
    BECAME_OPEN = 'became_open'
    BECAME_THROTTLED = 'became_throttled'
    STILL_THROTTLED = 'still_throttled'
    STILL_OPEN = 'still_open'


STATE_CHANGE_MAP = {
    # Key: (did change, current state)
    (True, True): StateChange.BECAME_OPEN,
    (True, False): StateChange.BECAME_THROTTLED,
    (False, True): StateChange.STILL_OPEN,
    (False, False): StateChange.STILL_THROTTLED,
}


class Rate:
    __slots__ = ('rate', 'period', 'rate_per_period')

    def __init__(self, rate: int, period: Union[float, int] = 1) -> None:
        self.rate = float(rate)
        self.period = float(period)
        if self.rate < 0:
            raise ValueError(f'`rate` must be >= 0 (not {self.rate!r})')
        if self.period <= 0:
            raise ValueError(f'`period` must be > 0 (not {self.period!r})')
        self.rate_per_period = (self.rate / self.period)

    def __repr__(self) -> str:
        return f'<Rate {self.rate:f} per {self.period:f}>'


class TickResult:
    """
    The result of a `.tick()` operation.

    If the `.state` value is True, the event should be allowed;
    if it is False, the event should not be allowed.
    (As a shorthand, `TickResult`s are truthy iff `.state` is True.)

    To find out whether the `.tick()` operation caused the state to change,
    and how, the `.did_change` value can be accessed; to find out the exact
    change state (as a `StateChange` value), it's available as `.state_change`.
    """

    __slots__ = ('state', 'did_change')

    def __init__(self, state: bool, did_change: bool) -> None:
        self.state = bool(state)
        self.did_change = bool(did_change)

    @property
    def state_change(self) -> StateChange:
        return STATE_CHANGE_MAP[(self.did_change, self.state)]

    def __bool__(self) -> bool:
        return self.state

    def __repr__(self) -> str:
        state_text = ('throttled' if not self.state else 'open')
        return f'<TickResult: {state_text} (change: {self.state_change})>'


class RateLimiter:
    """
    A token bucket -based rate limiter.

    See: https://en.wikipedia.org/wiki/Token_bucket
    Loosely based on https://stackoverflow.com/a/668327/51685
    """

    #: The clock to use for RateLimiters. Should return seconds (or whatever is
    #: the `period` of the RateLimiter) as a floating-point number.
    #: By default, the high-resolution performance counter is used.
    #: This can be overwritten, or overridden in subclasses.
    clock = (time.perf_counter if hasattr(time, 'perf_counter') else time.time)

    __slots__ = ('rate', 'allow_underflow', 'last_check', 'allowance', 'current_state')

    def __init__(self, rate: Rate, allow_underflow: bool = False) -> None:
        """
        :param rate: The Rate for this RateLimiter.
        :param allow_underflow: Whether to allow underflow for the limiter, i.e.
                                whether subsequent ticks during throttling may cause
                                the "token counter", as it were, to go negative.
        """
        self.rate = rate
        self.allow_underflow = bool(allow_underflow)
        self.last_check = None  # type: Optional[float]
        self.allowance = None  # type: Optional[float]
        self.current_state = None  # type: Optional[bool]

    @classmethod
    def from_per_second(cls, per_second: int, allow_underflow: bool = False) -> "RateLimiter":
        return cls(rate=Rate(rate=per_second), allow_underflow=allow_underflow)

    def _tick(self) -> bool:
        # https://github.com/python/mypy/issues/6910
        current = self.clock()  # type: float  # type: ignore[misc]

        if self.current_state is None:
            self.last_check = current
            self.allowance = self.rate.rate
            self.current_state = None

        last_check = self.last_check  # type: float # type: ignore[assignment]
        time_passed = current - last_check
        self.last_check = current
        self.allowance += time_passed * self.rate.rate_per_period  # type: ignore[operator]
        self.allowance = min(self.allowance, self.rate.rate)  # Do not allow allowance to grow unbounded
        throttled = (self.allowance < 1)
        if self.allow_underflow or not throttled:
            self.allowance -= 1

        return (not throttled)

    def reset(self) -> None:
        """
        Reset the rate limiter to an open state.
        """
        self.current_state = self.allowance = self.last_check = None

    def tick(self) -> TickResult:
        """
        Tick the rate limiter, i.e. when a new event should be processed.

        :return: Returns a TickResult; see that class's documentation for information.
        """
        new_state = self._tick()

        if self.current_state is None:
            self.current_state = new_state

        did_change = (new_state is not self.current_state)
        self.current_state = new_state

        return TickResult(new_state, did_change)

    def __repr__(self) -> str:
        state_text = ('throttled' if not self.current_state else 'open')
        return f'<RateLimiter {state_text} (allowance {self.allowance}, rate {self.rate})>'


class MultiRateLimiter:
    """
    Wraps multiple RateLimiters in a map.
    """

    rate_limiter_class = RateLimiter
    allow_underflow = False

    def __init__(self, default_limit: Rate, per_name_limits: Optional[Dict[str, Rate]] = None) -> None:
        self.limiters = {}  # type: Dict[str, RateLimiter]
        self.default_limit = default_limit
        self.per_name_limits = dict(per_name_limits or {})
        assert isinstance(default_limit, Rate)
        assert all(isinstance(l, Rate) for l in self.per_name_limits.values())

    def tick(self, name: str) -> TickResult:
        """
        Tick a named RateLimiter.

        :param name: Name of the limiter.
        :return: TickResult for the limiter.
        """
        return self.get_limiter(name).tick()

    def reset(self, name: str) -> bool:
        """
        Reset (i.e. delete) a named RateLimiter.
        :param name: Name of the limiter.
        :return: True if the limiter was found and deleted.
        """
        return bool(self.limiters.pop(name, None))

    def get_limiter(self, name: str) -> RateLimiter:
        """
        Get (or instantiate) a named RateLimiter.

        :param name: Name of the limiter.
        """
        limiter = self.limiters.get(name)
        if not limiter:
            limiter = self.limiters[name] = self.rate_limiter_class(
                rate=self.get_rate(name),
                allow_underflow=self.allow_underflow,
            )
        return limiter

    def get_rate(self, name: str) -> Rate:
        """
        Get the RateLimit for a named RateLimiter.

        This function is a prime candidate for overriding in a subclass.

        :param name: Name of the limiter.
        """
        return self.per_name_limits.get(name, self.default_limit)
