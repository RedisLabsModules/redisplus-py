import pytest
from redis import Redis
from redisplus.client import RedisClient

@pytest.fixture
def client():
    rc = RedisClient(modules={'redistimeseries': {"client": Redis()}})
    rc.REDISTIMESERIES.flushdb()
    return rc.REDISTIMESERIES

@pytest.mark.redistimeseries
def test_base(client):

    # base load
    rc = RedisClient(client=Redis())
    rc.flushdb()
