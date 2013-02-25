import pytest
from _helper import consume, tick, count_tasks

notimplemented = xfail = pytest.mark.xfail


from waterf import queue, task


messages = []

@pytest.fixture
def clear_messages():
    while messages:
        messages.pop()


def Ok(message='Ok'):
    return message
def RETURN_ABORT():
    return queue.ABORT
def CALL_ABORT():
    queue.ABORT("ABORT NOW")

def handler(message):
    messages.append(message)


@pytest.mark.usefixtures("clear_messages")
class TestCallbacks:
    def testSuccess(self, taskqueue):
        task(Ok).success(handler).run()
        assert ['Ok'] == messages

    def testCantAddTheSameHandlerTwice(self, taskqueue):
        task(Ok).success(handler).success(handler).run()
        assert ['Ok'] == messages


    def testDeferredHandler(self, taskqueue, ndb):
        task(Ok).success(task(handler)).run()
        assert count_tasks(taskqueue) == 1
        consume(taskqueue)
        assert ['Ok'] == messages

    def testFailure(self, taskqueue):
        task(RETURN_ABORT).failure(handler).run()

        assert [queue.ABORT] == messages

    def testCallAbortToAbort(self, taskqueue):
        task(CALL_ABORT).then(None, handler).run()

        assert "ABORT NOW" == str(messages[0])
        assert isinstance(messages[0], queue.AbortQueue)


    def testInOrderReturnsLastResult(self, taskqueue, ndb):
        queue.inorder(
            task(Ok, 'ONE'), task(Ok, 'TWO')
        ).then(handler).run()
        consume(taskqueue)

        assert ['TWO'] == messages

    @notimplemented
    def testParallelReturnsAllResults(self, taskqueue, ndb):
        queue.parallel(
            task(Ok, 'ONE'), task(Ok, 'TWO')
        ).then(handler).run()
        consume(taskqueue)

        assert [["ONE", "TWO"]] == messages
