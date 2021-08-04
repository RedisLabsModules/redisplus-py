import pytest
from redis import Redis
from redisplus.client import RedisClient

@pytest.fixture
def client():
    rc = RedisClient(modules={'redistimeseries': {"client": Redis()}})
    rc.REDISJSON.flushdb()
    return rc.REDISTIMESERIES

@pytest.mark.redistimeseries
def test_base():

    # base load
    rc = RedisClient(client=Redis())

    # try not to break the regular client init
    rc = RedisClient(modules={'redistimeseries': {"client": Redis()}})
