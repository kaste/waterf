A convenience module on top of the deferred library that comes with the Google AppEngine (GAE).

Note that you have to enable the deferred library in your app.yaml

::

    builtins:
    - deferred: on

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
    ).run()

Should be pretty self-explanatory: it first runs the function ``check_condition``, then it runs the function ``remove`` three times in parallel, after that it runs ``email``.

To abort execution of a series you either raise ``queue.PermanentTaskFailure`` or as a convenience return ``queue.ABORT``.

You use ``task()`` exactly the same as you used ``deferred.defer()``::

    task(check, id=102, _countdown=20)
    task(email, to='foo@bar.com', _queue='mailer')

After constructing a task you either ``run()`` or ``enqueue()`` it; whereby::

	task(foo, 'bar').enqueue()  <==> deferred.defer(foo, 'bar')
	task(foo, 'bar').run()      <==> foo('bar')

Read the tests. Thank you.