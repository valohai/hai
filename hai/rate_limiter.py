import time
from enum import Enum


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

    def __init__(self, rate, period=1):
        self.rate = float(rate)
        self.period = float(period)
        if self.rate < 0:
            raise ValueError('`rate` must be >= 0 (not %r)' % self.rate)
        if self.period <= 0:
            raise ValueError('`period` must be > 0 (not %r)' % self.period)
        self.rate_per_period = (self.rate / self.period)

    def __repr__(self):
        return '<Rate %f per %f>' % (self.rate, self.period)


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

    def __init__(self, state, did_change):
        self.state = bool(state)
        self.did_change = bool(did_change)

    @property
    def state_change(self):
        return STATE_CHANGE_MAP[(self.did_change, self.state)]

    def __bool__(self):
        return self.state

    def __repr__(self):
        return '<TickResult: %s (change: %s)>' % (
            ('throttled' if not self.state else 'open'),
            self.state_change,
        )


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

    def __init__(self, rate, allow_underflow=False):
        """
        :param rate: The Rate for this RateLimiter.
        :type rate: Rate
        :param allow_underflow: Whether to allow underflow for the limiter, i.e.
                                whether subsequent ticks during throttling may cause
                                the "token counter", as it were, to go negative.
        :type allow_underflow: bool
        """
        self.rate = rate
        self.allow_underflow = bool(allow_underflow)
        self.last_check = None
        self.allowance = None
        self.current_state = None

    @classmethod
    def from_per_second(cls, per_second, allow_underflow=False):
        return cls(rate=Rate(rate=per_second), allow_underflow=allow_underflow)

    def _tick(self):
        current = self.clock()

        if self.current_state is None:
            self.last_check = current
            self.allowance = self.rate.rate
            self.current_state = None

        time_passed = current - self.last_check
        self.last_check = current
        self.allowance += time_passed * self.rate.rate_per_period
        self.allowance = min(self.allowance, self.rate.rate)  # Do not allow allowance to grow unbounded
        throttled = (self.allowance < 1)
        if self.allow_underflow or not throttled:
            self.allowance -= 1

        return (not throttled)

    def reset(self):
        """
        Reset the rate limiter to an open state.
        """
        self.current_state = self.allowance = self.last_check = None

    def tick(self):
        """
        Tick the rate limiter, i.e. when a new event should be processed.

        :return: Returns a TickResult; see that class's documentation for information.
        :rtype: TickResult
        """
        new_state = self._tick()

        if self.current_state is None:
            self.current_state = new_state

        did_change = (new_state is not self.current_state)
        self.current_state = new_state

        return TickResult(new_state, did_change)

    def __repr__(self):
        return '<RateLimiter %s (allowance %s, rate %s)>' % (
            ('throttled' if not self.current_state else 'open'),
            self.allowance,
            self.rate,
        )


class MultiRateLimiter:
    """
    Wraps multiple RateLimiters in a map.
    """

    rate_limiter_class = RateLimiter
    allow_underflow = False

    def __init__(self, default_limit, per_name_limits=None):
        self.limiters = {}
        self.default_limit = default_limit
        self.per_name_limits = dict(per_name_limits or {})
        assert isinstance(default_limit, Rate)
        assert all(isinstance(l, Rate) for l in self.per_name_limits.values())

    def tick(self, name):
        """
        Tick a named RateLimiter.

        :param name: Name of the limiter.
        :return: TickResult for the limiter.
        :rtype: TickResult
        """
        return self.get_limiter(name).tick()

    def reset(self, name):
        """
        Reset (i.e. delete) a named RateLimiter.
        :param name: Name of the limiter.
        :return: True if the limiter was found and deleted.
        :rtype: bool
        """
        return bool(self.limiters.pop(name, None))

    def get_limiter(self, name):
        """
        Get (or instantiate) a named RateLimiter.

        :param name: Name of the limiter.
        :rtype: RateLimiter
        """
        limiter = self.limiters.get(name)
        if not limiter:
            limiter = self.limiters[name] = self.rate_limiter_class(
                rate=self.get_rate(name),
                allow_underflow=self.allow_underflow,
            )
        return limiter

    def get_rate(self, name):
        """
        Get the RateLimit for a named RateLimiter.

        This function is a prime candidate for overriding in a subclass.

        :param name: Name of the limiter.
        :rtype: Rate
        """
        return self.per_name_limits.get(name, self.default_limit)
