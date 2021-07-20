from redisplus.client import RedisClient
import contextlib
import redis
from utils import mockredisclient
import pytest

@pytest.mark.redisearch
def test_client_init():
    modules = {'redisearch': {'index_name': 'testtheindex'}}

    with pytest.raises(TypeError):
        RedisClient(from_url="redis://localhost:6379")

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

    with contextlib.ExitStack() as stack:
        stack = mockredisclient(stack)
        # create our own client
        rc = RedisClient(modules, from_url="redis://localhost:6379")
        assert rc.CLIENT is not None