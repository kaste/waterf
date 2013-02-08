import pytest
from _helper import consume, tick, count_tasks

notimplemented = xfail = pytest.mark.xfail


from waterf import queue, task




def noop(): pass

class TestQueueing:
    def testAllowDoubleExecution(self, taskqueue):
        task(noop).enqueue()
        task(noop).enqueue()
        assert count_tasks(taskqueue) == 2

    def testProtectByGivenId(self, taskqueue, ndb):
        task(noop).enqueue('A')
        task(noop).enqueue('A')
        assert count_tasks(taskqueue) == 1

    @notimplemented
    def testProtect(self, taskqueue, ndb):
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



