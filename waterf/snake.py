
"""
Experimental trash, and jet set.

A ndb.tasklet-style interface to waterf.queue

::

    from waterf import snake

    def A(data):
        rv = yield snake.task(other_func, data)
        rv2 = yield (
            snake.task(B, rv),
            snake.task(C)
        )
        raise snake.Return(rv2)

    snake.task(A, 'data').enqueue()


The implementation idea is to have the queue.tasks cache their results.

Note that in this pseudo-continuation-implementation any code which is not
a yield, gets executed multiple times.

"""

from google.appengine.ext import ndb
from google.appengine.ext import blobstore
from google.appengine.api import files

import pickle
import types
import logging
logger = logging.getLogger(__name__)

from . import queue

class Return(StopIteration):
    def __init__(self, rv):
        self.rv = rv

LARGE_PAYLOAD = 1000000

class _Result(ndb.Model):
    status = ndb.StringProperty(default='pending', indexed=False)
    large = ndb.BlobProperty(indexed=False)
    huge = ndb.BlobKeyProperty(indexed=False)

    @classmethod
    def _get_kind(cls):
        return '_Waterf_Result'

    def is_ready(self):
        return self.status == 'resolved'

    @property
    def result(self):
        if self.large:
            pickled = self.large
        elif self.huge:
            pickled = blobstore.BlobReader(self.huge).read()

        return pickle.loads(pickled)

    @result.setter
    def result(self, value):
        pickled = pickle.dumps(value, pickle.HIGHEST_PROTOCOL)
        if len(pickled) < LARGE_PAYLOAD:
            self.large = pickled
            return

        filename = files.blobstore.create(mime_type='application/octet-stream')
        with files.open(filename, 'a') as f:
            f.write(value)
        files.finalize(filename)

        self.huge = files.blobstore.get_blob_key(filename)

    def delete(self):
        if self.huge:
            blobstore.BlobInfo(self.huge).delete()

        self.key.delete()

    @ndb.transactional
    def resolve(self, result):
        self.key.get()
        self.status = 'resolved'
        self.result = result
        self.put()

class Result(queue.Lock):
    model = _Result

    def delete(self):
        if self.exists():
            self.get().delete()

    def is_ready(self):
        return self.exists() and self.get().is_ready()

    def resolve(self, value):
        if self.exists():
            self.get().resolve(value)

    def get_result(self):
        if self.is_ready():
            return self.get().result
        else:
            raise Pending


class Pending(Exception): pass

class Task(queue.Task):
    _Lock = Result

    def __init__(self, f, *a, **kw):
        super(Task, self).__init__(f, *a, **kw)
        self.id = self._generate_id()
        self.root = None

    def is_root(self):
        return self.root is None

    @property
    def future(self):
        return Result(self.id)

    def resolve(self, result):
        self.future.resolve(result)
        super(Task, self).resolve(result)

    def is_ready(self):
        return self.future.is_ready()

    def get_result(self):
        return self.future.get_result()

    def run(self):
        # child tasks are enqueued using _name, so they're not
        # mark'ed_as_enqueued yet
        if not self.is_root():
            self.mark_as_enqueued()

        try:
            rv = self.callable(*self.args, **self.kwargs)
        except queue.AbortQueue, e:
            rv = e
        except queue.PermanentTaskFailure, e:
            self.abort(e)
            raise
        except Return, e:
            # as a convenience, otherwise if you commented out the last yield
            # you would have to rewrite your function to use the standard 'return X'
            rv = e.rv

        if type(rv) is not types.GeneratorType:
            if rv is queue.ABORT:
                self.abort(rv)
            elif isinstance(rv, queue.AbortQueue):
                self.abort(rv)
            elif isinstance(rv, queue.Deferred):
                self.enqueue_subtask(rv)
            else:
                self.resolve(rv)
            return rv


        coroutine = rv
        message = None
        while 1:
            try:
                rv = coroutine.send(message)
            except Return, e:
                self.resolve(e.rv)
                return
            except StopIteration:
                self.resolve(None)
                return

            if isinstance(rv, Task):
                try:
                    message = rv.get_result()
                except Pending:
                    self.enqueue_subtask(rv)
                    return
            elif hasattr(rv, '__iter__'):
                assert all(isinstance(thing, Task) for thing in rv)

                parallel = Parallel(*rv)
                try:
                    message = parallel.get_result()
                except Pending:
                    self.enqueue_subtask(parallel)
                    return


    def enqueue_subtask(self, task):
        task.root = self.root if self.root else self
        task.root.always(task._cleanup_handler())
        super(Task, self).enqueue_subtask(task)

    def _cleanup(self, _):
        logger.debug("Cleanup %s" % self)
        self._lock.delete()

    def _subtask_completed(self, message):
        self.run()

task = Task

class Parallel(queue.Parallel):
    def __init__(self, *a, **kw):
        super(Parallel, self).__init__(*a, **kw)
        self.id = self._generate_id()

    def is_ready(self):
        return all(task.is_ready() for task in self.tasks)

    @property
    def result(self):
        return [task.get_result() for task in self.tasks]

    def get_result(self):
        if self.is_ready():
            return self.result
        else:
            raise Pending

    def enqueue_subtask(self, task):
        task.root = self.root if self.root else self
        task.root.always(task._cleanup_handler())
        super(Parallel, self).enqueue_subtask(task)




