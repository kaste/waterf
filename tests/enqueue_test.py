import pytest
from _helper import consume, tick, count_tasks

notimplemented = xfail = pytest.mark.xfail


from waterf import queue, task



def P(message=None):
    pass
def noop(): pass

class TestQueueing:
    def testDisableProtection(self, taskqueue):
        task(noop).enqueue(id=None)
        task(noop).enqueue(id=None)
        assert count_tasks(taskqueue) == 2

    def testSilentlyDisallowDoubleExecution(self, taskqueue, ndb):
        task(noop).enqueue()
        task(noop).enqueue()
        assert count_tasks(taskqueue) == 1

    def testProtectedByGivenId1(self, taskqueue, ndb):
        task(noop, _id='A').enqueue()
        task(noop, _id='A').enqueue()
        assert count_tasks(taskqueue) == 1

    def testIdCanBeReusedImmediately(self, taskqueue, ndb):
        task(noop).enqueue()
        assert count_tasks(taskqueue) == 1
        consume(taskqueue)
        task(noop).enqueue()
        assert count_tasks(taskqueue) == 1

    def testProtectedByGivenId(self, taskqueue, ndb):
        task(noop).enqueue('A')
        task(noop).enqueue('A')
        assert count_tasks(taskqueue) == 1

    def testGeneratedIdHandlesParameters(self, taskqueue, ndb):
        task(P, 'ONE').enqueue()
        task(P, 'ONE').enqueue()
        assert count_tasks(taskqueue) == 1
        task(P, 'TWO').enqueue()
        assert count_tasks(taskqueue) == 2

    def testPreventNamedTaskToBeScheduledTwice(self, taskqueue, ndb):
        task(noop).enqueue(_name='A')
        assert count_tasks(taskqueue) == 1
        task(noop).enqueue(_name='A')
        assert count_tasks(taskqueue) == 1



