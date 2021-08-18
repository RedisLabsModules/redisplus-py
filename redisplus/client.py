from typing import Dict, Optional
from redis.client import Redis
import redisplus.bf
import redisplus.json
import redisplus.ts


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

        :param modules: A list of dictionaries for modules to configure and their values.
            eg: {'redisearch': {'index_name': 'foo'}, ...}
        :type modules: dist
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
        return redisplus.json.Client(self.client)

    @property
    def bloom(self):
        """For running bloom specific commands."""
        return redisplus.bf.Client(self.client)