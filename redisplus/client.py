from typing import Dict, Optional
from redis.client import Redis
from redis.commands import Commands


class Client(Commands, object):
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
        self.client = client

        self.__extras__ = extras

    @property
    def __client__(self):
        """Return the internal representation of the redis client
        used for this connection. This is not public."""
        return self.client

    @property
    def json(self):
        """For running json commands."""
        kwargs = self.__extras__.get("json", {})
        import redisplus.json

        return redisplus.json.JSON(self.client, **kwargs)

    @property
    def bf(self):
        """For running bloom commands."""
        kwargs = self.__extras__.get("bf", {})
        import redisplus.bf

        return redisplus.bf.Bloom(self.client, **kwargs)

    @property
    def tf(self):
        """For running timeseries commands."""
        kwargs = self.__extras__.get("ts", {})
        import redisplus.ts

        return redisplus.ts.TimeSeries(self.client, **kwargs)

    @property
    def ai(self):
        """For running ai commands."""
        kwargs = self.__extras__.get("ai", {})
        import redisplus.ai

        return redisplus.ai.AI(self.client, **kwargs)

    @property
    def ft(self):
        """For running search commands."""
        kwargs = self.__extras__.get("search", {})
        import redisplus.search

        return redisplus.search.Search(self.client, **kwargs)

    def execute_command(self, *args, **kwargs):
        """Pull in and execute the redis commands"""
        return self.__client__.execute_command(*args, **kwargs)

    def pipeline(self, *args, **kwargs):
        """Pipelines"""
        return self.__client__.pipeline(*args, **kwargs)
