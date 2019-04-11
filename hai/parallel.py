import os
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

    def wait(self, fail_fast=True, interval=0.1):
        """
        Wait until all of the current tasks have finished.

        If `fail_fast` is True and any of them raises an exception,
        the exception is reraised within a ParallelException.
        In this case, the rest of the tasks will continue to run.

        :param fail_fast: Whether to abort the `wait` as
                          soon as a task crashes.
        :type fail_fast: bool

        :param interval: `.join()` check interval.
        :type interval: float

        :raises ParallelException: If any task crashes (only when
                                   fail_fast is true).
        """
        while True:
            all_dead = True
            for task in self.tasks:
                if not task.ready():
                    all_dead = False
                    continue
                if fail_fast and not task.successful():
                    message = '[%s] %s' % (task.name, str(task._value))
                    raise ParallelException(message) from task._value
                task.wait(interval)
            if all_dead:
                break

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
