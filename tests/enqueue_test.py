import pytest
from _helper import consume, tick, count_tasks

notimplemented = xfail = pytest.mark.xfail
beforeEach = pytest.mark.usefixtures

import datetime
from waterf import queue, task, snake

messages = []

@pytest.fixture
def clear_messages():
    while messages:
        messages.pop()


def P(message='P'):
    messages.append(message)
def noop(): pass


class TestQueueing:
    class TestNoProtection:
        def testDisableProtection2(self, taskqueue):
            task(noop).enqueue(use_id=False)
            task(noop).enqueue(use_id=False)
            assert count_tasks(taskqueue) == 2

        def testDisableProtection3(self, taskqueue):
            task(noop, _use_id=False).enqueue()
            task(noop, _use_id=False).enqueue()
            assert count_tasks(taskqueue) == 2

    class TestProtectByName:
        def testPreventNamedTaskToBeScheduledTwice(self, taskqueue):
            task(noop).enqueue(name='A')
            task(noop).enqueue(name='A')
            assert count_tasks(taskqueue) == 1

        def testPreventNamedTaskToBeScheduledTwice2(self, taskqueue):
            task(noop, _name='A').enqueue()
            task(noop, _name='A').enqueue()
            assert count_tasks(taskqueue) == 1

    class TestProtectById:
        def testSilentlyDisallowDoubleExecution(self, taskqueue, ndb):
            task(noop).enqueue()
            task(noop).enqueue()
            assert count_tasks(taskqueue) == 1

        def testProtectedByGivenId(self, taskqueue, ndb):
            task(noop).enqueue(use_id='A')
            task(noop).enqueue(use_id='A')
            assert count_tasks(taskqueue) == 1

        def testProtectedByGivenId2(self, taskqueue, ndb):
            task(noop, _use_id='A').enqueue()
            task(noop, _use_id='A').enqueue()
            assert count_tasks(taskqueue) == 1

        def testIdCanBeReusedImmediately(self, taskqueue, ndb):
            task(noop).enqueue()
            assert count_tasks(taskqueue) == 1
            consume(taskqueue)
            task(noop).enqueue()
            assert count_tasks(taskqueue) == 1

        def testIdIsReleasedAfterXSeconds(self, taskqueue, ndb):
            task(noop).enqueue(release_after=1)
            assert count_tasks(taskqueue) == 1
            tick(taskqueue)
            assert count_tasks(taskqueue) == 1  # the cleanup handler

            cleanup_handler = taskqueue.get_filtered_tasks()[0]

            now = datetime.datetime.now(tz=queue.taskqueue.taskqueue._UTC)
            now = now.replace(microsecond=0)
            assert cleanup_handler.eta == now + datetime.timedelta(seconds=1)

            task(noop).enqueue()
            assert count_tasks(taskqueue) == 1

            tick(taskqueue)
            assert count_tasks(taskqueue) == 0  # ensure

            task(noop).enqueue()
            assert count_tasks(taskqueue) == 1

        def testEnsureMultipleTaskGetCleanedIfReleaseAfterIsIused(
                self, taskqueue, fastndb):
            queue.inorder(
                task(P, 'ONE')
            ).enqueue(release_after=1)
            queue.inorder(
                task(P, 'TWO')
            ).enqueue(release_after=1)

            consume(taskqueue)
            semaphores = queue.Lock.model.query().fetch()
            assert len(semaphores) == 0

            # 1/0

        def testGeneratedIdHandlesParameters(self, taskqueue, ndb):
            task(P, 'ONE').enqueue()
            task(P, 'TWO').enqueue()
            assert count_tasks(taskqueue) == 2

        def testGeneratedIdHandlesParameters2(self, taskqueue, ndb):
            task(P, message='ONE').enqueue()
            task(P, message='TWO').enqueue()
            assert count_tasks(taskqueue) == 2

        class TestSubtasks:
            def testParentUsesId1(self, taskqueue):
                main = task(noop)
                sub = task(P)
                main.enqueue_subtask(sub)
                main.enqueue_subtask(sub)

                assert count_tasks(taskqueue) == 1

            def testParentUsesId2(self, taskqueue):
                main = task(noop, _use_id='A')
                sub = task(P)
                main.enqueue_subtask(sub)
                main.enqueue_subtask(sub)

                assert count_tasks(taskqueue) == 1

            def testParentUsesName1(self, taskqueue):
                main = task(noop, _name='A')
                sub = task(P)
                main.enqueue_subtask(sub)
                main.enqueue_subtask(sub)

                assert count_tasks(taskqueue) == 1

            def testParentUsesName2(self, taskqueue):
                main = task(noop)
                sub = task(P)
                main.enqueue_subtask(sub)
                main.enqueue_subtask(sub)

                assert count_tasks(taskqueue) == 1

            @beforeEach("clear_messages")
            def testA(self, taskqueue, ndb):
                A = queue.inorder(
                    task(P)
                ).enqueue(use_id='A')
                B = queue.inorder(
                    task(P)
                ).enqueue(use_id='B')
                assert count_tasks(taskqueue) == 2

                tick(taskqueue)
                assert count_tasks(taskqueue) == 2
                tick(taskqueue)
                assert count_tasks(taskqueue) == 0

                assert 'P P'.split() == messages

                # 1/0





