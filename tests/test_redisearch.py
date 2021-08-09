import pytest
from redis.client import Redis
from redis import Redis
from src.redisplus.client import RedisClient

@pytest.fixture
def client():
    rc = RedisClient(modules={'redisearch': {"client": Redis()}})
    rc.redisjson.flushdb()
    return rc.redisjson

@pytest.mark.redisearch
def test_base():

    # base load
    rc = RedisClient(client=Redis())

    # try not to break the regular client init
    rc = RedisClient(modules={'redisearch': {'client': Redis()}})
