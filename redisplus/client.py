from typing import Dict, Optional
from redis.client import Redis


class RedisPlus(object):
    """General client to be used for redis modules."""

    # list of active commands
    __commands__ = []

    def __init__(
        self,
        client: Optional[Redis] = None,
        extras: Optional[Dict] = {},
    ):
        """
        General client to be used for redis modules.

        :param client: An optional redis.client.Redis. If defined
                       this client will be used for all redis connections.
                       If this is not defined, one will be created.
        :type client: Redis
        """
        if client is None:
            client = Redis()
        self.__client__ = client

        self.__extras__ = extras

    @property
    def client(self):
        """Return the redis client, used for this connection."""
        return self.__client__

    @property
    def json(self):
        """For running json specific commands."""
        kwargs = self.__extras__.get("json", {})
        import redisplus.json
        return redisplus.json.Client(self.client, **kwargs)

    @property
    def bloom(self):
        """For running bloom specific commands."""
        kwargs = self.__extras__.get("bf", {})
        import redisplus.bf
        return redisplus.bf.Client(self.client, **kwargs)

    @property
    def timeseries(self):
        """For running bloom specific commands."""
        kwargs = self.__extras__.get("ts", {})
        import redisplus.ts
        return redisplus.ts.Client(self.client, **kwargs)

    @property
    def ai(self):
        """For running bloom specific commands."""
        kwargs = self.__extras__.get("ai", {})
        import redisplus.ai
        return redisplus.ai.Client(self.client, **kwargs)