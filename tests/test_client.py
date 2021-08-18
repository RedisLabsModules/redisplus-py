from redisplus import RedisPlus
import redis

def test_client_init():
    modules = {'json': {'client': redis.Redis()}}

    rc = RedisPlus(modules)
    assert getattr(rc, "json", None) is not None

    rc = RedisPlus(client=redis.Redis())
    assert getattr(rc, "json", None) is None
    assert isinstance(getattr(rc, "CLIENT", None), redis.Redis)

    rc = RedisPlus(client=redis.from_url("redis://localhost:6379"))
    assert isinstance(getattr(rc, "CLIENT", None), redis.Redis)

def test_module_list():
    modules = {'json': {}}
    rc = RedisPlus(modules, redis.Redis())
    assert 'json' in rc.modules

def test_refresh():
    cl = redis.Redis()
