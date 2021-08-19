from redisplus import RedisPlus
from redis import Redis


def test_client_init():
    assert isinstance(RedisPlus().client , Redis)

    assert isinstance(RedisPlus(Redis()).client , Redis)