import os
import threading
import time
from multiprocessing.pool import ApplyResult, ThreadPool
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from weakref import WeakSet


class ParallelException(Exception):
    pass


class TaskFailed(ParallelException):
    def __init__(self, message: str, task: "ApplyResult[Any]", exception: Exception) -> None:
        super().__init__(message)
        self.task = task
        self.task_name = str(getattr(task, "name", None))
        self.exception = exception


class TasksFailed(ParallelException):
    def __init__(self, message: str, exception_map: Dict[str, Exception]) -> None:
        super().__init__(message)
        self.exception_map = exception_map

    @property
    def failed_task_names(self) -> Set[str]:
        return set(self.exception_map)


class ParallelRun:
    """
    Encapsulates running several functions in parallel.

    Due to the GIL, this is mainly useful for IO-bound threads, such as
    downloading stuff from the Internet, or writing big buffers of data.

    It's recommended to use this in a `with` block, to ensure the thread pool
    gets properly shut down.
    """

    def __init__(self, parallelism: Optional[int] = None) -> None:
        self.pool = ThreadPool(processes=(parallelism or (int(os.cpu_count() or 1) * 2)))
        self.task_complete_event = threading.Event()
        self.tasks = []  # type: List[ApplyResult[Any]]
        self.completed_tasks = WeakSet()  # type: WeakSet[ApplyResult[Any]]

    def __enter__(self) -> "ParallelRun":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[no-untyped-def]
        self.pool.terminate()

    def __del__(self) -> None:  # opportunistic cleanup
        if self.pool:
            self.pool.terminate()
            self.pool = None  # type: ignore[assignment]

    def _set_task_complete_event(self, value: Any = None) -> None:
        self.task_complete_event.set()

    def add_task(
        self,
        task: Callable[..., None],
        name: Optional[str] = None,
        args: Tuple[Any, ...] = (),
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> "ApplyResult[Any]":
        """
        Begin running a function (in a secondary thread).

        :param task: The function to run.
        :param name: A name for the task.
                     If none is specified, it's derived from the callable.
        :param args: Positional arguments, if any.
        :param kwargs: Keyword arguments, if any.
        """
        if not name:
            name = (getattr(task, '__name__' or None) or str(task))  # type: ignore[arg-type]
        p_task = self.pool.apply_async(
            task,
            args=args,
            kwds=(kwargs or {}),
            callback=self._set_task_complete_event,
            error_callback=self._set_task_complete_event,
        )
        setattr(p_task, "name", str(name))  # noqa: B010
        self.tasks.append(p_task)

        # Clear completed tasks, in case someone calls `add_task`
        # while `.wait()` is in progress.  This will of course cause `.wait()`
        # to have to do some extra work, but that's fine.
        self.completed_tasks.clear()

        return p_task

    def wait(
        self,
        fail_fast: bool = True,
        interval: float = 0.5,
        callback: Optional[Callable[["ParallelRun"], None]] = None,
        max_wait: Optional[float] = None
    ) -> List["ApplyResult[Any]"]:
        """
        Wait until all of the current tasks have finished,
        or until `max_wait` seconds (if set) has been waited for.

        If `fail_fast` is True and any of them raises an exception,
        the exception is reraised within a ParallelException.
        In this case, the rest of the tasks will continue to run.

        :param fail_fast: Whether to abort the `wait` as
                          soon as a task crashes.

        :param interval: Loop sleep interval.

        :param callback: A function that is called on each wait loop iteration.
                         Receives one parameter, the parallel run
                         instance itself.

        :param max_wait: Maximum wait time, in seconds. Infinity if not set or zero.

        :raises TaskFailed: If any task crashes (only when fail_fast is true).
        :raises TimeoutError: If max_wait seconds have elapsed.
        """

        # Keep track of tasks we've certifiably seen completed,
        # to avoid having to acquire a lock for the `ready` event
        # when we don't need to.
        self.completed_tasks.clear()

        start_time = time.time()

        while True:
            if max_wait:
                waited_for = (time.time() - start_time)
                if waited_for > max_wait:
                    raise TimeoutError(f"Waited for {waited_for}/{max_wait} seconds.")

            had_any_incomplete_task = self._wait_tick(fail_fast)

            if callback:
                callback(self)

            # If there were no incomplete tasks left last iteration, quit.
            if not had_any_incomplete_task:
                break

            # If the number of completed tasks equals the number of tasks to process,
            # we're likewise done.
            if len(self.completed_tasks) == len(self.tasks):
                break

            # Otherwise wait for a bit before trying again (unless a task completes)
            self.task_complete_event.wait(interval)
            # Reset the flag in case it had been set
            self.task_complete_event.clear()

        return list(self.completed_tasks)  # We can just as well return the completed tasks.

    def _wait_tick(self, fail_fast: bool) -> bool:
        # Keep track of whether there were any incomplete tasks this loop.
        had_any_incomplete_task = False
        for task in self.tasks:  # :type: ApplyResult
            # If we've already seen this task completed, don't bother.
            if task in self.completed_tasks:
                continue

            # Poll the task to see if it's ready.
            is_ready = task.ready()

            if not is_ready:
                # If it's not yet ready, we need to loop once more,
                # and we can't check for success now.
                had_any_incomplete_task = True
                continue

            # Mark this task as completed (for good or for worse),
            # so we don't need to re-check it.
            self.completed_tasks.add(task)

            # Raise an exception if we're failing fast.
            # We're accessing `_success` directly instead of using
            # `.successful()` to avoid re-locking (as `.ready()` would).
            # Similarly, we access `._value` in order to avoid actually
            # raising the exception directly.
            if fail_fast and not task._success:  # type: ignore[attr-defined]
                exc = task._value  # type: ignore[attr-defined]
                message = f'[{task.name}] {str(exc)}'  # type: ignore[attr-defined]
                raise TaskFailed(
                    message,
                    task=task,
                    exception=exc,
                ) from exc
        return had_any_incomplete_task

    def maybe_raise(self) -> None:
        """
        Raise a `TasksFailed` if any of the run tasks
        ended up raising an exception.
        """
        exceptions = self.exceptions
        if exceptions:
            raise TasksFailed(
                '%d exceptions occurred' % len(exceptions),
                exception_map=exceptions,
            )

    @property
    def return_values(self) -> Dict[str, Any]:
        """
        Get the return values (if resolved yet) of the tasks.
        :return: dictionary of name to return value.
        """
        return {t.name: t._value for t in self.tasks if t.ready()}  # type: ignore[attr-defined]

    @property
    def exceptions(self) -> Dict[str, Exception]:
        """
        Get the exceptions (if any) of the tasks.

        :return: dictionary of task name to exception.
        """
        return {
            t.name: t._value  # type: ignore[attr-defined]
            for t in self.tasks
            if t.ready() and not t._success  # type: ignore[attr-defined]
        }
