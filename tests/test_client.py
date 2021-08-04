from redisplus.client import RedisClient
import redis

def test_client_init():
    modules = {'redisjson': {'client': redis.Redis()}}

    rc = RedisClient(modules)
    assert getattr(rc, "REDISJSON", None) is not None

    rc = RedisClient(client=redis.Redis())
    assert getattr(rc, "REDISJSON", None) is None
    assert isinstance(getattr(rc, "CLIENT", None), redis.Redis)

    rc = RedisClient(client=redis.from_url("redis://localhost:6379"))
    assert isinstance(getattr(rc, "CLIENT", None), redis.Redis)

def test_module_list():
    modules = {'redisjson': {}}
    rc = RedisClient(modules, redis.Redis())
    assert 'redisjson' in rc.modules

def test_refresh():
    cl = redis.Redis()