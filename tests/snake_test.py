import pytest
beforeEach = pytest.mark.usefixtures
notimplemented = xfail = pytest.mark.xfail

from _helper import consume, tick, count_tasks



from waterf.snake import _Result, Return, task, Parallel

from waterf import queue

messages = []

@pytest.fixture
def clear_messages():
    while messages:
        messages.pop()

def _clean():
    assert _Result.query().fetch() == []

@pytest.fixture
def ensure_cleaned_up(fastndb, request):
    request.addfinalizer(_clean)

def A():
    return 'A'

def B():
    raise Return('B')

def RaisePermanentFailure():
    raise queue.PermanentTaskFailure()

def ReturnAbort():
    return queue.ABORT

def RaiseReturnAbort():
    raise Return(queue.ABORT)

def CallAbort():
    queue.ABORT()

def YieldPlainValue():
    yield 'VALUE'
    raise Return('VALUE')

def Y(task):
    rv = yield task
    messages.append(rv)
    raise Return(rv)


@beforeEach("clear_messages")
# @beforeEach("ensure_cleaned_up")
class TestSnake:
    def testA(self, taskqueue, ndb):
        task(Y, task(A)).run()
        consume(taskqueue)

        assert ['A'] == messages
        assert _Result.query().fetch() == []

    def testB(self, taskqueue, ndb):
        task(Y, task(B)).run()
        consume(taskqueue)

        assert ['B'] == messages
        assert _Result.query().fetch() == []

    def testRaisePermanentFailure(self, taskqueue, fastndb):
        task(Y, task(RaisePermanentFailure)).run()
        consume(taskqueue)

        assert [] == messages
        assert _Result.query().fetch() == []

    def testRaiseReturnAbort(self, taskqueue, ndb):
        task(Y, task(ReturnAbort)).run()
        consume(taskqueue)

        assert [] == messages
        assert _Result.query().fetch() == []

    def testCallAbort(self, taskqueue, ndb):
        task(Y, task(CallAbort)).run()
        consume(taskqueue)

        assert [] == messages
        assert _Result.query().fetch() == []

    def testYieldPlainValue(self, taskqueue, ndb):
        task(Y, task(YieldPlainValue)).run()
        consume(taskqueue)

        assert ['VALUE'] == messages
        assert _Result.query().fetch() == []

def ECHO(message):
    return message

def T(message):
    rv = yield task(ECHO, message)
    yield task(ECHO, '1'), task(ECHO, '2')
    rv = yield task(ECHO, rv + '1'), task(ECHO, rv + '2')
    rv = yield task(ECHO, rv)
    messages.append(rv)
    raise Return(rv)

def Level2():
    messages.append('Level2')

def Level1():
    messages.append('Level1')
    yield task(Level2)

def Nested():
    messages.append('Nested')
    yield task(Level1)
    messages.append('Done')

def ONCE():
    messages.append('ONCE')

def CACHED():
    yield task(ONCE)


@beforeEach("clear_messages")
class TestSnakeFunctional:
    def testT(self, taskqueue, fastndb):
        task(T, 'T').enqueue()
        consume(taskqueue)

        assert [['T1', 'T2']] == messages
        assert _Result.query().fetch() == []
        assert Parallel.Semaphore.query().fetch() == []
        # 1/0

    def testNested(self, taskqueue, fastndb):
        task(Nested).enqueue()
        consume(taskqueue)

        assert "Nested Level1 Level2 Level1 Nested Done".split() == messages
        assert _Result.query().fetch() == []

    def testB(self, taskqueue, ndb):
        A = task(CACHED).enqueue(name='A')
        B = task(CACHED).enqueue(name='B')
        assert count_tasks(taskqueue) == 2

        tick(taskqueue)
        assert count_tasks(taskqueue) == 2
        tick(taskqueue)
        assert count_tasks(taskqueue) == 0

        assert 'ONCE ONCE'.split() == messages
        # 1/0


