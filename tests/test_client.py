from redisplus.client import RedisClient
import contextlib
import redis
import mock

def test_clients():
    modules = {'redisearch': {'index_name': 'testtheindex'}}

    # having the client created
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch("redis.connection.Connection.connect"))
        stack.enter_context(mock.patch("redis.connection.ConnectionPool.get_connection"))
        stack.enter_context(mock.patch("redis.connection.HiredisParser.can_read"))
        stack.enter_context(mock.patch("redis.exceptions.ConnectionError"))
        rc = RedisClient(modules=modules)
        assert rc.REDISEARCH_CLIENT is not None
        assert rc.CLIENT is not None

        # create our own client
        rc = RedisClient(modules=modules, client=redis.Redis(port=56789))
        assert rc.CLIENT is not None