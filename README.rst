A convenience module on top of the deferred library that comes with the Google AppEngine (GAE).

In a nutshell::

    from waterf import queue, task

    queue.inorder(
        task(check_condition),
        queue.parallel(
            task(remove, id=101),
            task(remove, id=102),
            task(remove, id=103)
        ),
        task(email, to='foo@bar.com')
    ).enqueue()

Should be pretty self-explanatory: it first runs the function ``check_condition``, then it runs the function ``remove`` three times in parallel, after that it runs ``email``.

To abort execution of a series you either raise ``queue.PermanentTaskFailure`` or as a convenience return ``queue.ABORT``. If you return another task, you further defer so to speak: the original task will get resolved (or aborted) as soon as the new (returned) task gets resolved (or aborted).

You use ``task()`` exactly the same as you used ``deferred.defer()``::

    task(check, id=102, _countdown=20)
    task(email, to='foo@bar.com', _queue='mailer')

After constructing a task you ``enqueue()`` it; the relation to the deferred.defer is roughly speaking::

    task(foo, 'bar').enqueue()  <==> deferred.defer(foo, 'bar')
    task(foo, 'bar').run()      <==> foo('bar')

Enqueue'ing takes (again) the same options defer took, overruling the ones you used in the constructor, e.g.::

    task(foo).enqueue(queue='mailer', countdown=60)

waterf adds two options::

    use_id  True | False | str
            Use if you don't come up with a good name to prevent double-scheduling
            The value True means autogenerate a good id, otherwise takes your str
            Defaults to True if a name is not set, otherwise to False

    release_after <seconds>
            Determines when the id will be released after your task has finished
            Defaults to 0, immediately

Tasks implement a jquery-like callback interface::

    task(foo).then(email_user, email_admin).always(...)

The callbacks must accept as their first argument the message the task sent. But this message passing will likely be dropped in a future version, because it's unused by the library.

On top of the waterf.queue there is some experimental jet set in the waterf.snake module, which implements a ndb.tasklet like api::

    from waterf import snake

    def A():
        raise snake.Return('A')

    def B(): ...

    def work():
        anA = yield snake.task(A)
        yield snake.task(B), snake.task(C) ...  # parallel yield

    snake.task(work).enqueue()




Note that you have to enable the deferred library in your app.yaml

::

    builtins:
    - deferred: on

Thank you.