import pytest
from _helper import consume, tick, count_tasks

notimplemented = xfail = pytest.mark.xfail


from waterf import queue, task



messages = []

@pytest.fixture(autouse=True)
def clear_messages():
    while messages:
        messages.pop()



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
def DEFER():
    messages.append('DEFER')
    return task(P, 'deferred')


@pytest.mark.usefixtures("clear_messages")
class TestWater:

    def testInOrder(self, taskqueue, ndb):
        queue.inorder(
            task(A), task(B), task(C)
        ).run()

        assert count_tasks(taskqueue) == 1
        consume(taskqueue)

        assert ['A', 'B', 'C'] == messages

    def testParallel(self, taskqueue, ndb):
        queue.parallel(
            task(P, 'ONE'), task(P, 'TWO'), task(P, 'THREE')
        ).run()

        assert count_tasks(taskqueue) == 3
        consume(taskqueue)

        assert 'ONE TWO THREE'.split() == messages

    def testCombined(self, taskqueue, fastndb):
        queue.inorder(
            task(A),
            queue.parallel(task(P, 'ONE'), task(P, 'TWO'), task(P, 'THREE')),
            task(B)
        ).run()

        consume(taskqueue)

        assert 'A ONE TWO THREE B'.split() == messages

        assert [] == queue._Counter.query().fetch()

    def testNestedInOrder(self, taskqueue, ndb):
        queue.inorder(
            task(A),
            queue.inorder(task(P, 'ONE'), task(P, 'TWO')),
            task(C)
        ).run()

        consume(taskqueue)
        assert 'A ONE TWO C'.split() == messages

    def testNestedParallel(self, taskqueue, ndb):
        queue.inorder(
            task(A),
            queue.parallel(
                queue.parallel(task(P, 'ONE'), task(P, 'TWO'), task(P, 'THREE')),
                task(B)),
            task(C)
        ).run()

        consume(taskqueue)
        assert "A B ONE TWO THREE C".split() == messages

    def testAbortInOrder(self, taskqueue, ndb):
        queue.inorder(
            task(A), task(T), task(C)
        ).run()

        consume(taskqueue)
        assert ["A"] == messages

    def testAbortNestedInOrder(self, taskqueue, ndb):
        queue.inorder(
            queue.inorder(task(A), task(T), task(C)),
            task(B)
        ).run()

        consume(taskqueue)
        assert ["A"] == messages

    def testAbortParallel(self, taskqueue, fastndb):
        queue.parallel(
            task(P, 'ONE'), task(T), task(P, 'TWO')
        ).then(task(C)).run()

        consume(taskqueue)
        assert "ONE TWO".split() == messages

        assert [] == queue._Counter.query().fetch()

    def testAbortNestedParallel(self, taskqueue, fastndb):
        queue.parallel(
            queue.parallel(task(P, 'ONE'), task(T), task(P, 'TWO')),
            task(B)
        ).then(task(C)).run()

        consume(taskqueue)
        assert "B ONE TWO".split() == messages

        assert [] == queue._Counter.query().fetch()

    def testReturnAbortToAbort(self, taskqueue, ndb):
        queue.inorder(
            task(A),
            task(RETURN_ABORT),
            task(B)
        ).run()

        consume(taskqueue)

        assert ["A"] == messages


    def testComplicated(self, taskqueue, ndb):
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


    class TestFurtherDefer:
        def testFurtherDefer(self, taskqueue, ndb):
            task(DEFER).run()
            consume(taskqueue)

            assert "DEFER deferred".split() == messages

        def testEnsureInOrderExecution(self, taskqueue, ndb):
            queue.inorder(
                task(A),
                task(DEFER),
                task(B)
            ).run()
            consume(taskqueue)

            assert "A DEFER deferred B".split() == messages


class Foo(object):
    def __init__(self, m='bar'):
        self.m = m

    def bar(self):
        messages.append(self.m)

    def __repr__(self):
        return "Foo<m=%s>" % self.m

@pytest.mark.usefixtures("clear_messages")
class TestPickableTasks:
    def testA(self, taskqueue, ndb):
        foo = Foo()
        task(foo.bar).enqueue()
        consume(taskqueue)

        assert ['bar'] == messages




