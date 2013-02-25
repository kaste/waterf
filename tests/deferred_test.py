import pytest
from _helper import consume, tick, count_tasks

notimplemented = xfail = pytest.mark.xfail
beforeEach = pytest.mark.usefixtures


from waterf import deferred2 as deferred

messages = []

@pytest.fixture
def clear_messages():
    while messages:
        messages.pop()


def A(data):
    messages.append(data)

@beforeEach("clear_messages")
class TestPayloadStores:
    def testSmall(self, taskqueue, fastndb):
        deferred.defer(A, 'A')
        assert deferred._DeferredTaskEntity.query().fetch() == []

        consume(taskqueue)
        assert ['A'] == messages

    def testLarge(self, taskqueue, fastndb):
        data = 'A' * 100000
        deferred.defer(A, data)

        payload = deferred._DeferredTaskEntity.query().get()
        assert payload.large

        consume(taskqueue)
        assert [data] == messages
        assert deferred._DeferredTaskEntity.query().fetch() == []

    def testHuge(self, taskqueue, fastndb, blobstore):
        data = 'A' * 1000000
        deferred.defer(A, data)

        payload = deferred._DeferredTaskEntity.query().get()
        assert payload.huge

        consume(taskqueue)
        assert [data] == messages
        assert deferred._DeferredTaskEntity.query().fetch() == []
        assert blobstore.BlobInfo.all().fetch(limit=None) == []

@beforeEach("clear_messages")
class TestAdditionalCosmeticUrlArguments:
    def testAddsArgsToTheUrl(self, taskqueue):
        task = deferred.defer(A, 'A', _url_args='foo')
        assert task.url == deferred._DEFAULT_URL + "/foo"

        task = deferred.defer(A, 'A', _url_args=('foo'))
        assert task.url == deferred._DEFAULT_URL + "/foo"

        task = deferred.defer(A, 'A', _url_args=('foo',))
        assert task.url == deferred._DEFAULT_URL + "/foo"

        task = deferred.defer(A, 'A', _url_args=('foo','bar'))
        assert task.url == deferred._DEFAULT_URL + "/foo/bar"

    def testRemovesArgsBeforeCallingTheDeferred(self, taskqueue):
        task = deferred.defer(A, 'A', _url_args=('foo','bar'))
        consume(taskqueue)
        assert ['A'] == messages


