Documentation for redisplus
=====================================

Redisplus is the python interface for redis modules redismodules.io. This is a unified client using `redis-py <https://github.com/andymccurdy/redis-py>_` to send commands to redis, and access the modules.

Installation
-------------

To install redisplus run:

```pip install redisplus```

Quick Start
-----------

Connect using a pre-build redis client, for all modules::

    import redis
    from redisplus import RedisPlus

    r = redis.Redis()
    rc = RedisPlus(r)
    rc.client.status()

Access json features, setting a json key::

    import redis
    from redisplus import RedisPlus

    r = redis.Redis()
    rc = RedisPlus(modules={"redisjson": {"client": r}})
    rc.redisjson.jsonset("foo", ".", "bar")
    rc.redisjson.exists("foo")
