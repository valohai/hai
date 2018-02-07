from threading import Thread, Lock


class ParallelRunThread(Thread):
    def run(self):
        self.exception = False
        self.return_value = None
        try:
            self.return_value = self._target(*self._args, **self._kwargs)
        except Exception as exc:
            self.exception = exc
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs


class ParallelException(Exception):
    pass


class ParallelRun:
    """
    Encapsulates running several functions in parallel.
    """

    def __init__(self):
        self.threads = []

    def add_task(self, task, name=None, args=(), kwargs=None):
        """
        Begin running a function (in a secondary thread).

        :param task: The function to run.
        :type task: function
        :param name: A name for the task. If none is specified, it's derived from the task's name.
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

    def wait(self):
        """
        Wait until all of the current tasks have finished.

        If any of them raises an exception, the exception is reraised within a ParallelException.

        :raises ParallelException: If any task crashes.
        """
        while True:
            all_dead = True
            for thread in self.threads:
                if thread.exception:
                    raise ParallelException('[%s] %s' % (thread.name, str(thread.exception))) from thread.exception
                if not thread.is_alive():
                    continue
                all_dead = False
                thread.join(0.1)
            if all_dead:
                break

    @property
    def return_values(self):
        """
        Get the return values (if resolved yet) of the tasks.
        :return: dictionary of name to return value.
        """
        return {t.name: t.return_value for t in self.threads}
