import conftest


def pytest_funcarg__taskqueue(request):
    from google.appengine.ext import testbed
    bed = request.getfuncargvalue('bed')
    bed.init_taskqueue_stub(root_path=conftest.APP_ROOT)
    return bed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

def pytest_funcarg__ndb(request):
    from google.appengine.datastore import datastore_stub_util
    bed = request.getfuncargvalue('bed')
    bed.init_memcache_stub()
    policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=0)
    bed.init_datastore_v3_stub(consistency_policy=policy)

    from google.appengine.ext import ndb
    return ndb

def pytest_funcarg__fastndb(request):
    from google.appengine.datastore import datastore_stub_util
    bed = request.getfuncargvalue('bed')
    bed.init_memcache_stub()
    policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
    bed.init_datastore_v3_stub(consistency_policy=policy)

    from google.appengine.ext import ndb
    return ndb



def pytest_funcarg__bed(request):
    from google.appengine.ext import testbed
    bed = testbed.Testbed()
    bed.activate()
    request.addfinalizer(lambda: teardown_bed(bed))
    return bed

def teardown_bed(bed):
    bed.deactivate()


