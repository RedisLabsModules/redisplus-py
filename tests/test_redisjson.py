from redisplus.client import RedisClient
import redis
from utils import mockredisclient
import contextlib

def test_json_init():
    # try loading the module
    modules = {'redisjson': {}}
    rc = RedisClient(modules)

def test_command_loader():
    modules = {'redisjson': {}}
    rc = RedisClient(modules)
    assert len(rc.commands) > 1
    assert isinstance(rc.commands, list)

def test_called_function():
    modules = {'redisjson': {}}
    client = redis.Redis()
    with contextlib.ExitStack() as stack:
        stack = mockredisclient(stack)
        rc = RedisClient(modules, client=client)
        rc.jsondel(client, 'something')