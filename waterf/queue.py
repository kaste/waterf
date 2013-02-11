"""
watef is built on top of appengine's deferred library.

The relationship between the defined Task below and the deferred library
is as follows::

    t = task(foo, 'bar', **options)
    t.enqueue()  ==> deferred.defer(t.run, **options)
    t.run()      ==> foo('bar')

A task implements a jquery-like-callback-interface, where you can register
callbacks::

    t.success(cllb)
    t.failure(cllb)
    t.always(cllb)
    t.then(success_cllb, failure_cllb)

Note that you have to register your callbacks before enqueue'ing the task,
because the callbacks go to the server as well.

The callbacks are fired when the task
A callback can be an ordinary function (or method) or another task. The
latter one will be enqueued when the task gets resolved. The functions get
executed immediately.




"""



from google.appengine.api import taskqueue
from google.appengine.ext import deferred
from google.appengine.ext import ndb

import types, hashlib
import uuid
import logging
logger = logging.getLogger(__name__)



PermanentTaskFailure = deferred.PermanentTaskFailure
TaskAlreadyExistsError = taskqueue.TaskAlreadyExistsError

class AbortQueue(PermanentTaskFailure):
    pass

def ABORT(message):
    raise AbortQueue(message)

def formatspec(funcname=None, *args, **kwargs):
    spec = [str(funcname)]
    spec.extend([repr(a) for a in args])
    spec.extend(["%s=%s" % (k, (repr(v))) for k, v in kwargs.items()])
    return ', '.join(spec)

def _extract_options(kwargs):
    options = {}
    for option in ("_countdown", "_eta", "_headers", "_name", "_target",
            "_transactional", "_url", "_retry_options", "_queue"):
        if option in kwargs:
            options[option] = kwargs.pop(option)

    return kwargs, options

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
    elif isinstance(obj, _Task):
        return obj
    else:
        raise ValueError("obj must be callable")

class _Future(object):
    def __init__(self):
        self._success = []
        self._failure = []
        self._always = []

    def notify(self, type, message):
        callbacks = getattr(self, '_' + type, [])
        for callback in callbacks + self._always:
            # print callback, type
            # deferred.defer(invoke_callback, callback, message)
            invoke_callback(callback, message)

    def abort(self, message):
        self.notify('failure', message)

    def resolve(self, message):
        self.notify('success', message)


    def _add_callback(self, type, callback):
        getattr(self, '_' + type).append(curry_callback(callback))
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

class _Task(_Future):
    suppress_task_exists_error = True

    def __init__(self, **options):
        super(_Task, self).__init__()
        self.id = options.pop('_id', None)
        self.options = options

    def run(self):
        raise NotImplemented

    def is_enqueued(self):
        return _Semaphore.get_by_id(self.id) is not None

    def mark_as_enqueued(self):
        _Semaphore.get_or_insert(self.id)

    def enqueue_direct(self, **opts):
        logger.info('Enqueue %s' % self)

        options = self.options.copy()
        options.update(opts)

        return deferred.defer(self.run, **options)

    def enqueue(self, id=OMITTED, **options):
        if id is not OMITTED:
            self.id = id
        elif '_name' not in options and '_name' not in self.options:
            self.id = hashlib.md5("%s" % self).hexdigest()


        @ndb.transactional
        def tx():
            self.mark_as_enqueued()
            self.always(self._cleanup)

            options['_transactional'] = True
            return self.enqueue_direct(**options)


        try:
            if self.id:
                if self.is_enqueued():
                    raise TaskAlreadyExistsError
                return tx()
            else:
                return self.enqueue_direct(**options)
        except TaskAlreadyExistsError:
            if not self.suppress_task_exists_error:
                raise

    def enqueue_as_subtask(self, task):
        task.success(self._subtask_completed)  \
            .failure(self._subtask_failed)     \
            .enqueue()

    def _subtask_completed(self, message):
        raise NotImplemented
    def _subtask_failed(self, message):
        raise NotImplemented

    def _cleanup(self, message):
        ndb.Key(_Semaphore, self.id).delete()


class Task(_Task):
    def __init__(self, func, *args, **kwargs):
        self.target = curry_callback(func)
        self.args = args
        self.kwargs, options = _extract_options(kwargs)
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
            logger.info("%s sent ABORT" % self)
            self.abort(e)
            raise

        if rv is ABORT:
            self.abort(rv)
        elif isinstance(rv, AbortQueue):
            self.abort(rv)
        elif isinstance(rv, _Task):
            self.enqueue_as_subtask(rv)
        else:
            self.resolve(rv)
        return rv

    _subtask_completed = _Task.resolve
    _subtask_failed = _Task.abort

    def __repr__(self):
        return "Task(%s)" % formatspec(self.target, *self.args, **self.kwargs)

task = Task

class InOrder(_Task):
    def __init__(self, *tasks, **options):
        super(InOrder, self).__init__(**options)
        self.tasks = list(tasks)

    def run(self):
        task = self.tasks.pop(0)
        self.enqueue_as_subtask(task)

    def _subtask_completed(self, message):
        if self.tasks:
            self.run()
        else:
            self.resolve(message)

    _subtask_failed = _Task.abort

    def __repr__(self):
        return "InOrder(%s)" % formatspec(*self.tasks, **self.options)

inorder = InOrder


class _Counter(ndb.Model):
    _use_memcache = False
    counter = ndb.IntegerProperty(default=0, indexed=False)

    @classmethod
    def _get_kind(cls):
        return '_Waterf_Counter'


class Parallel(_Task):
    def __init__(self, *tasks, **options):
        super(Parallel, self).__init__(**options)

        self._uuid = str(uuid.uuid1())
        self.tasks = list(tasks)

    def run(self):
        self.completed = 0
        self.always(self._cleanup_counter)
        for task in self.tasks:
            self.enqueue_as_subtask(task)

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
        ndb.Key(_Counter, self._uuid).delete()

    def aborted(self):
        return _Counter.get_by_id(self._uuid) is None

    @property
    def completed(self):
        return _Counter.get_by_id(self._uuid).counter

    @completed.setter
    def completed(self, value):
        @ndb.transactional
        def tx():
            entity = _Counter.get_or_insert(self._uuid)
            entity.counter = value
            entity.put()
        tx()

    def __repr__(self):
        return "Parallel(%s)" % formatspec(*self.tasks, **self.options)

parallel = Parallel
