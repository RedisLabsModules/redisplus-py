import pytest
from redis import Redis
from src.redisplus import RedisClient

@pytest.fixture
def client():
    rc = RedisClient(modules={'redisbloom': {"client": Redis()}})
    rc.redisbloom.flushdb()
    return rc.redisbloom

@pytest.mark.redisbloom
def test_base():
    # try not to break the regular client init
    rc = RedisClient(modules={'redisbloom': {'client': Redis()}})
