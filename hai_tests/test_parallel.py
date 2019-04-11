import time

import pytest

from hai.parallel import ParallelException, ParallelRun


def agh():
    time.sleep(0.1)
    raise RuntimeError('agh!')


def return_true():
    return True


def test_parallel_crash():
    with ParallelRun() as parallel:
        parallel.add_task(return_true)
        parallel.add_task(agh)
        with pytest.raises(ParallelException) as ei:
            parallel.wait()
        assert str(ei.value.__cause__) == 'agh!'


def test_parallel_retval():
    with ParallelRun() as parallel:
        parallel.add_task(return_true, name='blerg')
        parallel.add_task(return_true)
        parallel.wait()
        assert parallel.return_values == {'blerg': True, 'return_true': True}


def test_parallel_wait_without_fail_fast():
    with ParallelRun() as parallel:
        parallel.add_task(return_true, name='true')
        parallel.add_task(agh, name='agh')
        parallel.wait(fail_fast=False)
        assert parallel.exceptions['agh'].args[0] == 'agh!'
        assert parallel.return_values['true'] is True
        with pytest.raises(ParallelException):
            parallel.maybe_raise()


@pytest.mark.parametrize('fail', (False, True))
def test_parallel_limit(fail):
    """
    Test that parallelism limits work.

    The `fail = True` mode checks that the test itself works.
    """
    count = 0

    def tick():
        nonlocal count
        assert count <= 3
        count += 1
        time.sleep(.1)
        count -= 1

    with ParallelRun(parallelism=(5 if fail else 3)) as parallel:
        for x in range(6):
            parallel.add_task(tick, name=str(x))
        if fail:
            with pytest.raises(ParallelException):
                parallel.wait(fail_fast=True)
        else:
            parallel.wait(fail_fast=True)
            assert count == 0
