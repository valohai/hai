import time

import pytest

from hai.parallel import ParallelException, ParallelRun


def agh():
    time.sleep(0.1)
    raise RuntimeError('agh!')


def return_true():
    return True


def test_parallel_crash():
    parallel = ParallelRun()
    parallel.add_task(return_true)
    parallel.add_task(agh)
    with pytest.raises(ParallelException) as ei:
        parallel.wait()
    assert str(ei.value.__cause__) == 'agh!'


def test_parallel_retval():
    parallel = ParallelRun()
    parallel.add_task(return_true, name='blerg')
    parallel.add_task(return_true)
    parallel.wait()
    assert parallel.return_values == {'blerg': True, 'return_true': True}
