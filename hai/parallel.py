import os
import threading
from weakref import WeakSet
from multiprocessing.pool import ThreadPool
from multiprocessing.dummy import Process as ProcessThread


class ParallelException(Exception):
    pass


class TaskFailed(ParallelException):
    def __init__(self, message, task, exception):
        super().__init__(message)
        self.task = task
        self.task_name = task.name
        self.exception = exception


class TasksFailed(ParallelException):
    def __init__(self, message, exception_map):
        super().__init__(message)
        self.exception_map = exception_map

    @property
    def failed_task_names(self):
        return set(self.exception_map)


class NameableThreadPool(ThreadPool):

    def Process(self, *args, **kwds):
        self.process_counter += 1
        if self.name:
            kwds.setdefault('name', self._format_name(self.process_counter))
        return ProcessThread(*args, **kwds)

    def _format_name(self, suffix):
        return '%s-%s' % (self.name, suffix)

    def __init__(self, *, name=None, processes=None, initializer=None, initargs=()):
        self.name = name
        self.process_counter = 0
        ThreadPool.__init__(self, processes, initializer, initargs)
        if self.name:
            # Unfortunately these won't affect e.g. PyCharm's concurrency diagram,
            # but they may nevertheless be useful.
            self._worker_handler.name = self._format_name('WorkerHandler')
            self._task_handler.name = self._format_name('TaskHandler')
            self._result_handler.name = self._format_name('ResultHandler')


class ParallelRun:
    """
    Encapsulates running several functions in parallel.

    Due to the GIL, this is mainly useful for IO-bound threads, such as
    downloading stuff from the Internet, or writing big buffers of data.

    It's recommended to use this in a `with` block, to ensure the thread pool
    gets properly shut down.
    """

    def __init__(self, parallelism=None, name=None):
        self.pool = NameableThreadPool(
            name=name,
            processes=(parallelism or (os.cpu_count() * 2)),
        )
        self.task_complete_event = threading.Event()
        self.tasks = []
        self.completed_tasks = WeakSet()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.terminate()

    def __del__(self):  # opportunistic cleanup
        if self.pool:
            self.pool.terminate()
            self.pool = None

    def _set_task_complete_event(self, value=None):
        self.task_complete_event.set()

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
        task = self.pool.apply_async(
            task,
            args=args,
            kwds=(kwargs or {}),
            callback=self._set_task_complete_event,
            error_callback=self._set_task_complete_event,
        )
        task.name = str(name)
        self.tasks.append(task)

        # Clear completed tasks, in case someone calls `add_task`
        # while `.wait()` is in progress.  This will of course cause `.wait()`
        # to have to do some extra work, but that's fine.
        self.completed_tasks.clear()

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

        :raises TaskFailed: If any task crashes (only when fail_fast is true).
        """

        # Keep track of tasks we've certifiably seen completed,
        # to avoid having to acquire a lock for the `ready` event
        # when we don't need to.
        self.completed_tasks.clear()

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
                    raise TaskFailed(message, task=task, exception=task._value) from task._value

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

    def maybe_raise(self):
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

        :return: dictionary of task name to exception.
        """
        return {
            t.name: t._value
            for t in self.tasks
            if t.ready() and not t._success
        }
