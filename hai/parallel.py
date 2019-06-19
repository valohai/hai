import os
import time
from weakref import WeakSet
from multiprocessing.pool import ThreadPool


class ParallelException(Exception):
    pass


class ParallelRun:
    """
    Encapsulates running several functions in parallel.

    Due to the GIL, this is mainly useful for IO-bound threads, such as
    downloading stuff from the Internet, or writing big buffers of data.

    It's recommended to use this in a `with` block, to ensure the thread pool
    gets properly shut down.
    """

    def __init__(self, parallelism=None):
        self.pool = ThreadPool(processes=(parallelism or (os.cpu_count() * 2)))
        self.tasks = []
        self.completed_tasks = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.terminate()

    def __del__(self):  # opportunistic cleanup
        if self.pool:
            self.pool.terminate()
            self.pool = None

    def add_task(self, task, name=None, args=(), kwargs=None):
        """
        Begin running a function (in a secondary thread).

        :param task: The function to run.
        :type task: function
        :param name: A name for the task.
                     If none is specified, it's derived from the callable.
        :type name: str|None
        :param args: Positional arguments, if any.
        :type args: tuple|list|None
        :param kwargs: Keyword arguments, if any.
        :type kwargs: dict|None
        """
        if not name:
            name = (getattr(task, '__name__' or None) or str(task))
        task = self.pool.apply_async(task, args=args, kwds=(kwargs or {}))
        task.name = str(name)
        self.tasks.append(task)
        return task

    def wait(self, fail_fast=True, interval=0.5, callback=None):
        """
        Wait until all of the current tasks have finished.

        If `fail_fast` is True and any of them raises an exception,
        the exception is reraised within a ParallelException.
        In this case, the rest of the tasks will continue to run.

        :param fail_fast: Whether to abort the `wait` as
                          soon as a task crashes.
        :type fail_fast: bool

        :param interval: Loop sleep interval.
        :type interval: float

        :param callback: A function that is called on each wait loop iteration.
                         Receives one parameter, the parallel run
                         instance itself.
        :type callback: function

        :raises ParallelException: If any task crashes (only when
                                   fail_fast is true).
        """

        # Keep track of tasks we've certifiably seen completed,
        # to avoid having to acquire a lock for the `ready` event
        # when we don't need to.

        self.completed_tasks = WeakSet()

        while True:
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
                if fail_fast and not task._success:
                    message = '[%s] %s' % (task.name, str(task._value))
                    raise ParallelException(message) from task._value

            if callback:
                callback(self)

            # If there were no incomplete tasks left last iteration, quit.
            if not had_any_incomplete_task:
                break

            # Otherwise wait for a bit before trying again.
            time.sleep(interval)

        return list(self.completed_tasks)  # We can just as well return the completed tasks.

    def maybe_raise(self):
        """
        Raise a `ParallelException` if any of the run tasks
        ended up raising an exception.
        """
        exceptions = self.exceptions
        if exceptions:
            exc = ParallelException('%d exceptions occurred' % len(exceptions))
            exc.exceptions = exceptions
            raise exc

    @property
    def return_values(self):
        """
        Get the return values (if resolved yet) of the tasks.
        :return: dictionary of name to return value.
        """
        return {t.name: t._value for t in self.tasks if t.ready()}

    @property
    def exceptions(self):
        """
        Get the exceptions (if any) of the tasks.
        :return: dictionary of name to exception.
        """
        return {
            t.name: t._value
            for t in self.tasks
            if t.ready() and not t._success
        }
