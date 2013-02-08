
from google.appengine.ext import deferred


__all__ = ('consume', 'tick', 'count_tasks', 'pytest_funcarg__taskqueue')


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



def pytest_funcarg__taskqueue(taskqueue):
    taskqueue.consume = lambda: consume(taskqueue)
    taskqueue.tick = lambda: tick(taskqueue)
    taskqueue.count_tasks = lambda: count_tasks(taskqueue)

    return taskqueue


