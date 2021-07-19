from redisplus.client import RedisClient
import contextlib
import redis
from utils import mockredisclient

def test_init():
    modules = {'redisearch': {'index_name': 'testtheindex'}}

    # limit validations to our library interaction - not redis
    with contextlib.ExitStack() as stack:
        stack = mockredisclient(stack)
        rc = RedisClient(modules=modules)
        assert rc.REDISEARCH_CLIENT is not None
        assert rc.CLIENT is not None

    with contextlib.ExitStack() as stack:
        stack = mockredisclient(stack)
        # create our own client
        rc = RedisClient(modules=modules, client=redis.Redis(port=56789))
        assert rc.CLIENT is not None