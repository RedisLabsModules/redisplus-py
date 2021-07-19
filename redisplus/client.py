from typing import Dict, Optional
from redis.client import Redis
import importlib


class RedisClient(object):
    """General client to be used for redis modules"""

    def __init__(self, modules: Dict, client=Optional[Redis]):
        """
        Creates an all-purpose client and passes them in to
        all of the resulting modules.

        :param modules: A dictionary of dictionaries for modules in the module
                        list.
                        eg: {'redisearch': {'index_name': 'foo'}, ...}
        :type modules: dict

        :param client: A pre-build client of type redis.Redis. This is used to
                       create all clients.
        :type client: redis.Redis
        """
        if client is not None:
            self.CLIENT = client
        else:
            self.CLIENT = Redis()

        for key, vals in modules.items():
            modclient = "{}_CLIENT".format(key.upper())
            x = importlib.import_module(key)
            setattr(self, modclient, x.Client(**vals))