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

        return redisplus.json.JSON(self.client, **kwargs)

    @property
    def bloom(self):
        """For running bloom specific commands."""
        kwargs = self.__extras__.get("bf", {})
        import redisplus.bf

        return redisplus.bf.Bloom(self.client, **kwargs)

    @property
    def timeseries(self):
        """For running bloom specific commands."""
        kwargs = self.__extras__.get("ts", {})
        import redisplus.ts

        return redisplus.ts.TimeSeries(self.client, **kwargs)

    @property
    def ai(self):
        """For running bloom specific commands."""
        kwargs = self.__extras__.get("ai", {})
        import redisplus.ai

        return redisplus.ai.AI(self.client, **kwargs)

    @property
    def search(self):
        """For running bloom specific commands."""
        kwargs = self.__extras__.get("search", {})
        import redisplus.search

        return redisplus.search.Search(self.client, **kwargs)

    def execute_command(self, *args, **kwargs):
        """Pull in and excecute the redis commands"""
        return self.client.execute_command(*args, **kwargs)

    def pipeline(self, *args, **kwargs):
        """Pipelines"""
        return self.client.pipeline(*args, **kwargs)
