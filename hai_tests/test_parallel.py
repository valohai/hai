import time
from unittest.mock import MagicMock

import pytest

from hai.parallel import ParallelException, ParallelRun, TaskFailed, TasksFailed


def agh():
    time.sleep(0.1)
    raise RuntimeError("agh!")


def return_true():
    return True


def run_chord(parallel: ParallelRun):
    chord = parallel.chord()
    chord.add_task(return_true, name="task_0")
    chord.add_task(return_true, name="task_1")
    chord.wait()
    return chord.return_values


def run_chord_with_error(parallel: ParallelRun):
    chord = parallel.chord()
    chord.add_task(return_true, name="task_0")
    chord.add_task(agh, name="task_1")
    try:
        chord.wait()
    except TaskFailed:
        # Wait for the remaining tasks to complete
        chord.wait(fail_fast=False)
    return chord.return_values


def test_parallel_crash():
    with ParallelRun() as parallel:
        parallel.add_task(return_true)
        failing_task = parallel.add_task(agh)
        with pytest.raises(TaskFailed) as ei:
            parallel.wait(fail_fast=True)
        assert str(ei.value.__cause__) == str(ei.value.exception) == "agh!"
        assert ei.value.task == failing_task


def test_parallel_retval():
    with ParallelRun() as parallel:
        parallel.add_task(return_true, name="blerg")
        parallel.add_task(return_true)
        parallel.wait()
        assert parallel.return_values == {"blerg": True, "return_true": True}


def test_parallel_wait_without_fail_fast():
    with ParallelRun() as parallel:
        parallel.add_task(return_true, name="true")
        parallel.add_task(agh, name="agh")
        parallel.wait(fail_fast=False)
        assert parallel.exceptions["agh"].args[0] == "agh!"
        assert parallel.return_values["true"] is True
        with pytest.raises(TasksFailed) as ei:
            parallel.maybe_raise()
        assert len(ei.value.exception_map) == 1
        assert isinstance(ei.value.exception_map["agh"], RuntimeError)
        assert ei.value.failed_task_names == {"agh"}


@pytest.mark.parametrize("is_empty_run", (False, True))
def test_parallel_callback_is_called_at_least_once_on_wait(is_empty_run):
    with ParallelRun() as parallel:
        stub = MagicMock()
        if not is_empty_run:
            parallel.add_task(return_true, name="true")
        parallel.wait(callback=stub)
        stub.assert_called_with(parallel)


@pytest.mark.parametrize("fail", (False, True))
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
        time.sleep(0.1)
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


def test_parallel_long_interval_interruptible():
    """
    Test that even with long poll intervals, completion events interrupt the sleep
    """
    with ParallelRun() as parallel:
        parallel.add_task(time.sleep, args=(0.5,))  # will only wait for half a second
        t0 = time.time()
        parallel.wait(interval=10)  # would wait for 10
        t1 = time.time()
        assert t1 - t0 < 5


def test_parallel_max_wait():
    with ParallelRun() as parallel:
        parallel.add_task(time.sleep, args=(1,))
        with pytest.raises(TimeoutError):
            parallel.wait(interval=0.1, max_wait=0.5)


def test_parallel_chord_task():
    with ParallelRun() as parallel:
        for i in range(3):
            parallel.add_task(
                run_chord,
                kwargs={"parallel": parallel},
                name="chord_%d" % i,
            )
        parallel.wait()
        assert parallel.return_values == {
            "chord_0": {"task_0": True, "task_1": True},
            "chord_1": {"task_0": True, "task_1": True},
            "chord_2": {"task_0": True, "task_1": True},
        }


def test_parallel_chord_task_fail():
    """
    Test that failures inside a chord don't interrupt other chords or tasks
    """
    with ParallelRun() as parallel:
        parallel.add_task(return_true)
        parallel.add_task(run_chord, kwargs={"parallel": parallel}, name="chord_ok")
        parallel.add_task(
            run_chord_with_error,
            kwargs={"parallel": parallel},
            name="chord_fail",
        )
        parallel.wait()
        assert parallel.return_values["return_true"]
        assert parallel.return_values["chord_ok"] == {"task_0": True, "task_1": True}
        assert parallel.return_values["chord_fail"]["task_0"]
        assert isinstance(parallel.return_values["chord_fail"]["task_1"], RuntimeError)
