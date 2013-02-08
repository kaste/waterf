import sys, os

APP_ROOT = os.path.realpath(__file__)
GAESDK_PATH = "c:\dev\gae"

def fix_sys_path(path):
    sys.path.insert(0, path)

    import dev_appserver
    dev_appserver.fix_sys_path()

def pytest_addoption(parser):
    group = parser.getgroup("gae", "google app engine plugin")
    group.addoption('--gaesdk', action='store', dest='gaesdk_path',
                    metavar='PATH', default="c:\dev\gae",
                    help="Google App Engine's root PATH")

def pytest_configure(config):
    fix_sys_path(config.option.gaesdk_path)


