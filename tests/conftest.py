from redis import Redis
import pytest

# to allow us to run all tests that have no markings as -m unmarked
def pytest_collection_modifyitems(items, config):
    for item in items:
        if not any(item.iter_markers()):
            item.add_marker("unmarked")


def skip_ifmodversion_lt(min_version: str, module_name: str):
    rc = Redis()
    modules = rc.execute_command("module list")
    if modules is None:
        return

    version = None
    for module_info in modules:
        if module_info[b"name"] == module_name.encode():
            version = int(module_info[b"ver"])

    if version is None:
        raise AttributeError("No redis module named {}".format(module_name))
    mv = int(min_version.replace(".", ""))
    check = version < mv
    return pytest.mark.skipif(check, reason="Redis module version")
