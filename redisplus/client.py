from typing import Optional
from redis.client import Redis


class RedisPlus(object):
    """General client to be used for redis modules."""

    # list of active commands
    __commands__ = []

    def __init__(
        self,
        client: Optional[Redis] = None,
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

    @property
    def client(self):
        """Return the redis client, used for this connection."""
        return self.__client__

    @property
    def json(self):
        """For running json specific commands."""
        import redisplus.json
        return redisplus.json.Client(self.client)

    @property
    def bloom(self):
        """For running bloom specific commands."""
        import redisplus.bf
        return redisplus.bf.Client(self.client)

    @property
    def timeseries(self):
        """For running bloom specific commands."""
        import redisplus.ts
        return redisplus.ts.Client(self.client)