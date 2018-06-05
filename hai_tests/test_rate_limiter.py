import time

import pytest

from hai.rate_limiter import MultiRateLimiter, Rate, RateLimiter, StateChange


def test_rate_limiter():
    l = RateLimiter.from_per_second(10)
    for x in range(12):
        r = l.tick()
        if x < 10:
            assert r
            assert r.state
            assert not r.did_change
            assert r.state_change == StateChange.STILL_OPEN
        elif x == 10:
            assert not r
            assert not r.state
            assert r.did_change
            assert r.state_change == StateChange.BECAME_THROTTLED
        elif x == 11:  # pragma: no branch
            assert not r
            assert not r.state
            assert not r.did_change
            assert r.state_change == StateChange.STILL_THROTTLED

    time.sleep(.02)  # Wait for a very short time...
    assert not l.tick()  # Still throttled...
    time.sleep(.2)  # Wait for enough to get 2 tokens.

    assert l.allowance < 1  # No tokens?
    r = l.tick()
    assert 1 <= l.allowance <= 2  # We used one token in the tick, but one and some should be left
    assert r
    assert r.did_change
    assert r.state_change == StateChange.BECAME_OPEN


def test_underflowable_rate_limiter():
    l = RateLimiter.from_per_second(10, allow_underflow=True)
    for x in range(20):
        r = l.tick()
        if x == 10:  # We should be out of tickets by now
            assert not r
    assert l.allowance < 0
    time.sleep(1)
    assert not l.tick()  # Still underflowing, yeah?
    assert l.allowance < 0
    time.sleep(1)
    assert l.tick()  # Back in the black!
    assert l.allowance >= 1


def test_limiter_reset():
    l = RateLimiter.from_per_second(1)
    assert l.tick()
    assert not l.tick()
    time.sleep(0.1)
    assert not l.tick()
    l.reset()  # So impatient... OwO
    assert l.tick()


def test_per_multi_second_limit():
    """
    Test that rate limits X per Y second rates are supported
    """
    l = RateLimiter(Rate(1, 2))
    assert l.tick().state_change == StateChange.STILL_OPEN
    assert l.tick().state_change == StateChange.BECAME_THROTTLED
    time.sleep(1)
    assert l.tick().state_change == StateChange.STILL_THROTTLED
    time.sleep(1)
    assert l.tick().state_change == StateChange.BECAME_OPEN


def test_zero_limiter():
    l = RateLimiter(Rate(0, 0.1))
    assert not l.tick()
    time.sleep(0.1)
    assert not l.tick()
    time.sleep(0.1)
    assert not l.tick()
    # Yeah okay I give up, it's not gonna happen


def test_rate_construction_validation():
    """
    Test that the Rate constructor validates its arguments
    """
    with pytest.raises(ValueError):
        Rate(-1, 1)
    with pytest.raises(ValueError):
        Rate(1, 0)


def test_multi_limiter():
    ml = MultiRateLimiter(default_limit=Rate(1, 0.1))
    # Tick two limiters:
    assert ml.tick('foo')
    assert ml.tick('bar')
    assert not ml.tick('foo')
    assert not ml.tick('bar')
    # Reset one of them and assert it is now open, yet the other is not
    assert ml.reset('foo')  # Check that it got reset
    assert ml.tick('foo')
    assert not ml.tick('bar')
    # Wait for the other to open up again
    time.sleep(0.11)
    assert ml.tick('bar')


def test_smoke_reprs():
    l = RateLimiter.from_per_second(1)
    assert isinstance(repr(l), str)
    assert isinstance(repr(l.rate), str)
    assert isinstance(repr(l.tick()), str)
