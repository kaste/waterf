"""
In a nutshell::

    from waterf import queue, task

    queue.inorder(
        task(check_condition),
        queue.parallel(
            task(remove, id=101),
            task(remove, id=102),
            task(remove, id=103)
        ),
        task(email, to='foo@bar.com')
    ).enqueue()



"""



from google.appengine.api import taskqueue
from google.appengine.ext import ndb
try:
    import deferred2 as deferred
except ImportError:
    from google.appengine.ext import deferred

import types, hashlib
import uuid
from collections import defaultdict
import logging
logger = logging.getLogger(__name__)



PermanentTaskFailure = deferred.PermanentTaskFailure
TaskAlreadyExistsError = taskqueue.TaskAlreadyExistsError

class AbortQueue(PermanentTaskFailure):
    pass

def ABORT(message=None):
    raise AbortQueue(message)

def formatspec(funcname=None, *args, **kwargs):
    spec = [str(funcname)]
    spec.extend([repr(a) for a in args])
    spec.extend(["%s=%s" % (k, (repr(v))) for k, v in kwargs.items()])
    return ', '.join(spec)

def invoke_callback(callable, message):
    if isinstance(callable, Task):
        callable.args = (message,) + callable.args
        return callable.enqueue()
    try:
        obj, methodname = callable
        callable = getattr(obj, methodname)
    except:
        pass
    return callable(message)

def curry_callback(obj):
    if isinstance(obj, types.MethodType):
        return (obj.im_self, obj.im_func.__name__)
    elif isinstance(obj, types.BuiltinMethodType):
        if not obj.__self__:
            return obj
        else:
            return (obj.__self__, obj.__name__)
    elif isinstance(obj, types.ObjectType) and hasattr(obj, "__call__"):
        return obj
    elif isinstance(obj, (types.FunctionType, types.BuiltinFunctionType,
                        types.ClassType, types.UnboundMethodType)):
        return obj
    elif isinstance(obj, Deferred):
        return obj
    else:
        raise ValueError("obj must be callable")

class _CallbacksInterface(object):
    """ A JQuery-like interface.
    Register your callbacks::

        task(...).success(cllb) \
                 .failure(cllb) \
                 .always(cllb)  \
                 .then(success_cllb, failure_cllb)

    Later when you either ``abort()`` or ``resolve()`` your task, the
    corresponding callbacks get fired.

    A callback can be an ordinary function (or method) or another task. The
    latter one will be enqueued when the task gets resolved. The functions get
    executed immediately.

    Note that you have to register your callbacks before enqueue'ing the task,
    because the callbacks go to the server as well.
    """
    def __init__(self):
        self.callbacks = defaultdict(set)

    def notify(self, type, message):
        callbacks = self.callbacks[type].copy() | self.callbacks['always']
        for callback in callbacks:
            # print callback, type
            # deferred.defer(invoke_callback, callback, message)
            invoke_callback(callback, message)

    def abort(self, message):
        logger.debug("Abort %s with %r" % (self, message))
        self.notify('failure', message)

    def resolve(self, message):
        logger.debug("Resolve %s with %r" % (self, message))
        self.notify('success', message)


    def _add_callback(self, type, callback):
        self.callbacks[type].add(curry_callback(callback))
        return self

    def success(self, callback):
        return self._add_callback('success', callback)

    def failure(self, callback):
        return self._add_callback('failure', callback)

    def always(self, callback):
        return self._add_callback('always', callback)

    def then(self, success, failure=None):
        if success is not None:
            self.success(success)
        if failure is not None:
            self.failure(failure)
        return self

class _Semaphore(ndb.Model):
    @classmethod
    def _get_kind(cls):
        return '_Waterf_Semaphore'

class Object(object): pass
OMITTED = Object()

class Deferred(_CallbacksInterface):
    """ Enqueue-Run-Interface

    The relationship with the deferred.defer library is as follows::

        t = task(foo, 'bar', **options)
        t.enqueue()  ==> deferred.defer(t.run, **options)
        t.run()      ==> foo('bar')

    The provided ``enqueue`` method though silently protects you against
    queue'in the same task multiple times before it finished.

    """
    suppress_task_exists_error = True
    Semaphore = _Semaphore

    def __init__(self, **options):
        super(Deferred, self).__init__()
        self.options = options
        self.uid = str(uuid.uuid1())

        self._release_after = self.options.pop('release_after', 0)

    def run(self):
        raise NotImplemented

    def is_enqueued(self):
        return self.Semaphore.get_by_id(self.id) is not None

    def mark_as_enqueued(self):
        self.Semaphore.get_or_insert(self.id)

    def enqueue_direct(self, **options):
        logger.info('Enqueue %s with %s' % (self, options))

        options = self._prefix_keys_with_(options)
        return deferred.defer(self.run, **options)

    def enqueue(self, **opts):
        options = self.options.copy()
        options.update(opts)

        if 'use_id' in options and 'name' in options:
            raise TypeError("Either set use_id or name, but not both")

        use_id = options.pop('use_id', False if 'name' in options else True)
        if 'release_after' in options:
            self._release_after = options.pop('release_after')

        try:
            if not use_id:
                return self.enqueue_direct(**options)
            else:
                self.id = self._generate_id() if use_id is True else use_id
                if self.is_enqueued():
                    raise TaskAlreadyExistsError

                self.always(self._cleanup_handler())

                task = self.enqueue_direct(**options)
                self.mark_as_enqueued()
                return task

        except TaskAlreadyExistsError, taskqueue.TombstonedTaskError:
            if not self.suppress_task_exists_error:
                raise
            logger.info("Task existed.")

    def enqueue_subtask(self, task):
        task.success(self._subtask_completed)  \
            .failure(self._subtask_failed)     \
            .enqueue_as_subtask()

    def _subtask_completed(self, message):
        self.resolve(message)

    def _subtask_failed(self, message):
        self.abort(message)

    def enqueue_as_subtask(self):
        if 'name' in self.options or 'use_id' in self.options:
            return self.enqueue()
        else:
            return self.enqueue(name=self.uid)

    def _cleanup_handler(self):
        if self._release_after == 0:
            return self._cleanup
        else:
            return task(self._cleanup, _countdown=self._release_after)

    def _cleanup(self, message):
        logger.debug("Cleanup %s" % self)
        ndb.Key(self.Semaphore, self.id).delete()

    def _generate_id(self):
        return hashlib.md5("%s" % self).hexdigest()

    def _prefix_keys_with_(self, options, prefix='_'):
        new_options = {}
        for key, value in options.items():
            if key.startswith(prefix):
                new_options[key] = value
            else:
                new_options[prefix + key] = value

        return new_options


class Task(Deferred):
    def __init__(self, func, *args, **kwargs):
        self.target = curry_callback(func)
        self.args = args
        self.kwargs, options = self._extract_options(kwargs)

        super(Task, self).__init__(**options)

    @property
    def callable(self):
        if isinstance(self.target, tuple):
            return getattr(*self.target)
        else:
            return self.target

    def run(self):
        try:
            rv = self.callable(*self.args, **self.kwargs)
        except AbortQueue, e:
            rv = e
        except PermanentTaskFailure, e:
            self.abort(e)
            raise

        if rv is ABORT:
            self.abort(rv)
        elif isinstance(rv, AbortQueue):
            self.abort(rv)
        elif isinstance(rv, Deferred):
            self.enqueue_subtask(rv)
        else:
            self.resolve(rv)
        return rv

    def _extract_options(self, kwargs):
        options = {}
        for option in ("_countdown", "_eta", "_headers", "_name", "_target",
                "_transactional", "_url", "_retry_options", "_queue",
                "_use_id", "_release_after"):
            if option in kwargs:
                # remove the prefix ('_')
                options[option[1:]] = kwargs.pop(option)

        return kwargs, options

    def __repr__(self):
        return "Task(%s)" % formatspec(self.target, *self.args, **self.kwargs)

task = Task

class InOrder(Deferred):
    def __init__(self, *tasks, **options):
        super(InOrder, self).__init__(**options)
        self.tasks = list(tasks)

    def run(self):
        task = self.tasks.pop(0)
        self.enqueue_subtask(task)

    def _subtask_completed(self, message):
        if self.tasks:
            self.run()
        else:
            self.resolve(message)

    def __repr__(self):
        return "InOrder(%s)" % formatspec(*self.tasks)

inorder = InOrder


class _Counter(ndb.Model):
    _use_memcache = False
    counter = ndb.IntegerProperty(default=0, indexed=False)

    @classmethod
    def _get_kind(cls):
        return '_Waterf_Counter'


class Parallel(Deferred):
    def __init__(self, *tasks, **options):
        super(Parallel, self).__init__(**options)
        self.tasks = list(tasks)

    def run(self):
        self.completed = 0
        self.always(self._cleanup_counter)
        for task in self.tasks:
            self.enqueue_subtask(task)

    def _subtask_completed(self, message):
        @ndb.transactional
        def complete():
            if self.aborted():
                return

            if self.completed == len(self.tasks) - 1:
                return True
            else:
                self.completed += 1

        if complete():
            self.resolve(message)

    def _subtask_failed(self, message):
        if not ndb.transaction(lambda: self.aborted()):
            self.abort(message)

    def _cleanup_counter(self, message):
        logger.debug('Delete Counter for %s' % self)
        ndb.Key(_Counter, self.uid).delete()

    def aborted(self):
        return _Counter.get_by_id(self.uid) is None

    @property
    def completed(self):
        return _Counter.get_by_id(self.uid).counter

    @completed.setter
    def completed(self, value):
        @ndb.transactional
        def tx():
            entity = _Counter.get_or_insert(self.uid)
            entity.counter = value
            entity.put()
        tx()

    def __repr__(self):
        return "Parallel(%s)" % formatspec(*self.tasks)

parallel = Parallel
