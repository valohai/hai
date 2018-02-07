from threading import Thread


class ParallelRunThread(Thread):
    def run(self):
        self.exception = False
        self.return_value = None
        self.finished = False
        try:
            self.return_value = self._target(*self._args, **self._kwargs)
        except Exception as exc:
            self.exception = exc
        finally:
            self.finished = True
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs


class ParallelException(Exception):
    pass


class ParallelRun:
    """
    Encapsulates running several functions in parallel.

    Due to the GIL, this is mainly useful for IO-bound threads, such as
    downloading stuff from the Internet, or writing big buffers of data.
    """

    def __init__(self):
        self.threads = []

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
        thread = ParallelRunThread(
            name=name,
            target=task,
            args=args,
            kwargs=kwargs,
            daemon=True,
        )
        thread.start()
        self.threads.append(thread)

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
            for thread in self.threads:
                if fail_fast and thread.exception:
                    message = '[%s] %s' % (thread.name, str(thread.exception))
                    raise ParallelException(message) from thread.exception
                if thread.finished:
                    continue
                all_dead = False
                thread.join(interval)
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
        return {t.name: t.return_value for t in self.threads if t.finished}

    @property
    def exceptions(self):
        """
        Get the exceptions (if any) of the tasks.
        :return: dictionary of name to exception.
        """
        return {
            t.name: t.exception
            for t in self.threads
            if t.finished and t.exception
        }
