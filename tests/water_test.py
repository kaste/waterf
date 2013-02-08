import pytest; xfail = pytest.mark.xfail
from funcargs import *



from google.appengine.api import taskqueue
from google.appengine.ext import deferred

from waterf import queue, task


messages = []


def clear_messages():
    for _ in range(len(messages)):
        messages.pop()

def pytest_funcarg__messages(request):
    global messages
    clear_messages()
    return messages


class NothingToDo(Exception):    pass

def consume(taskqueue):
    while 1:
        try:
            tick(taskqueue)
        except NothingToDo:
            break

def tick(taskqueue):
    tasks = taskqueue.get_filtered_tasks()
    if not tasks:
        raise NothingToDo()

    for task in tasks:
        try:
            deferred.run(task.payload)
        except deferred.PermanentTaskFailure:
            pass
        finally:
            taskqueue.DeleteTask(task.queue_name or 'default', task.name)

def count_tasks(taskqueue):
    return len(taskqueue.get_filtered_tasks())


def pytest_funcarg__taskq(request):
    taskqueue = request.getfuncargvalue('taskqueue')
    taskqueue.consume = lambda: consume(taskqueue)
    taskqueue.tick = lambda: tick(taskqueue)
    taskqueue.count_tasks = lambda: count_tasks(taskqueue)

    return taskqueue

def noop(): pass

class TestQueueing:
    def testAllowDoubleExecution(self, taskqueue, messages):
        task(noop).enqueue()
        task(noop).enqueue()
        assert count_tasks(taskqueue) == 2

    def testProtectByGivenId(self, taskqueue, messages, ndb):
        task(noop).enqueue('A')
        task(noop).enqueue('A')
        assert count_tasks(taskqueue) == 1

    @xfail
    def testProtect(self, taskqueue, messages, ndb):
        task(P, 'ONE').enqueue(protect=True)
        task(P, 'ONE').enqueue(protect=True)
        assert count_tasks(taskqueue) == 1
        task(P, 'TWO').enqueue(protect=True)
        assert count_tasks(taskqueue) == 1

    def testPreventNamedTaskToBeScheduledTwice(self, taskqueue):
        task(noop).enqueue(_name='A')
        assert count_tasks(taskqueue) == 1
        task(noop).enqueue(_name='A')
        assert count_tasks(taskqueue) == 1



def RET(message='RET'):
    return message

def handler(message):
    messages.append(message)

class TestMessaging:
    def testSuccess(self, taskqueue, messages):
        task(RET).success(handler).run()
        assert ['RET'] == messages

    def testDeferredHandler(self, taskqueue, messages):
        task(RET).success(task(handler)).run()
        consume(taskqueue)
        assert ['RET'] == messages

    def testFailure(self, taskqueue, messages):
        task(RETURN_ABORT).failure(handler).run()

        assert [queue.ABORT] == messages

    def testCallAbortToAbort(self, taskqueue, messages):
        task(CALL_ABORT).then(None, handler).run()

        assert "ABORT NOW" == str(messages[0])
        assert isinstance(messages[0], queue.AbortQueue)



    def testInOrderReturnsLastResult(self, taskqueue, messages):
        queue.inorder(
            task(RET, 'ONE'), task(RET, 'TWO')
        ).then(handler).run()
        consume(taskqueue)

        assert ['TWO'] == messages

    @xfail
    def testParallelReturnsAllResults(self, taskqueue, messages, ndb):
        queue.parallel(
            task(RET, 'ONE'), task(RET, 'TWO')
        ).then(handler).run()
        consume(taskqueue)

        assert [["ONE", "TWO"]] == messages

def DEFER():
    messages.append('DEFER')
    return task(P, 'deferred')

class TestFurtherDefer:
    def testFurtherDefer(self, taskqueue, messages):
        task(DEFER).run()
        consume(taskqueue)

        assert "DEFER deferred".split() == messages

    def testEnsureInOrderExecution(self, taskqueue, messages):
        queue.inorder(
            task(A),
            task(DEFER),
            task(B)
        ).run()
        consume(taskqueue)

        assert "A DEFER deferred B".split() == messages

def A():    messages.append('A')
def B():    messages.append('B')
def C():    messages.append('C')
def T():    raise queue.PermanentTaskFailure()
def RETURN_ABORT():
    return queue.ABORT
def CALL_ABORT():
    queue.ABORT("ABORT NOW")
def P(message='P'):
    messages.append(message)

class TestWater:

    def testInOrder(self, taskqueue, ndb):
        clear_messages()

        queue.inorder(
            task(A), task(B), task(C)
        ).run()

        assert count_tasks(taskqueue) == 1

        consume(taskqueue)

        assert ['A', 'B', 'C'] == messages

    def testParallel(self, taskqueue, ndb):
        clear_messages()

        queue.parallel(
            task(P), task(P), task(P)
        ).run()

        assert count_tasks(taskqueue) == 3

        consume(taskqueue)

        assert 'P P P'.split() == messages

    def testCombined(self, taskqueue, fastndb):
        assert [] == queue._Counter.query().fetch()
        clear_messages()

        queue.inorder(
            task(A),
            queue.parallel(task(P), task(P), task(P)),
            task(B)
        ).run()

        # assert count_tasks(taskqueue) == 1
        # tick(taskqueue)
        # assert count_tasks(taskqueue) == 1  # that's the parallel
        # tick(taskqueue)
        # assert count_tasks(taskqueue) == 3  # that's the content of the paralell
        # tick(taskqueue)
        # assert count_tasks(taskqueue) == 1
        # tick(taskqueue)
        # assert count_tasks(taskqueue) == 0

        consume(taskqueue)

        assert ['A', 'P', 'P', 'P', 'B'] == messages

        assert [] == queue._Counter.query().fetch()

    def testNestedInOrder(self, taskqueue, ndb):
        clear_messages()

        queue.inorder(
            task(A),
            queue.inorder(task(A), task(B), task(C)),
            task(C)
        ).run()

        consume(taskqueue)
        assert 'A A B C C'.split() == messages

    def testNestedParallel(self, taskqueue, ndb):
        clear_messages()

        queue.inorder(
            task(A),
            queue.parallel(
                task(B),
                queue.parallel(task(P), task(P), task(P)),
                task(B)),
            task(C)
        ).run()

        consume(taskqueue)
        assert "A B B P P P C".split() == messages

    def testAbortInOrder(self, taskqueue, ndb):
        clear_messages()

        queue.inorder(
            task(A), task(T), task(C)
        ).run()

        consume(taskqueue)
        assert ["A"] == messages

    def testAbortNestedInOrder(self, taskqueue, ndb):
        clear_messages()

        queue.inorder(
            queue.inorder(task(A), task(T), task(C)),
            task(B)
        ).run()

        consume(taskqueue)
        assert ["A"] == messages

    def testAbortParallel(self, taskqueue, fastndb):
        clear_messages()

        queue.parallel(
            task(P), task(T), task(P)
        ).then(task(C)).run()

        consume(taskqueue)
        assert "P P".split() == messages

        assert [] == queue._Counter.query().fetch()

    def testAbortNestedParallel(self, taskqueue, fastndb):
        clear_messages()

        queue.parallel(
            queue.parallel(task(P), task(T), task(P)),
            task(B)
        ).then(task(C)).run()

        consume(taskqueue)
        assert "B P P".split() == messages

        assert [] == queue._Counter.query().fetch()

    def testReturnAbortToAbort(self, taskqueue, messages):
        queue.inorder(
            task(A),
            task(RETURN_ABORT),
            task(B)
        ).run()

        consume(taskqueue)

        assert ["A"] == messages


    def testComplicated(self, taskqueue, ndb):
        clear_messages()

        queue.parallel(
            task(A),
            queue.inorder(
                task(P, 'ONE'),
                task(P, 'TWO'),
                task(P, 'THREE')
            ),
            task(B)
        ).run()

        consume(taskqueue)
        assert "A B ONE TWO THREE".split() == messages

    def testTaskStr(self):

        print task(P, message='ONE')
        print queue.inorder(task(A), task(B))
        print queue.parallel(task(A), task(B), task(P, message='ONE'))

        # assert False


