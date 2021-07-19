from typing import Dict, Optional
from redis import Redis
import importlib
import logging

class RedisClient(object):
    """General client to be used for redis modules"""

    def __init__(self, modules: Dict, client=None, logger_name=None): #: Optional(Redis)):
        """
        Creates an all-purpose client and passes them in to all of the resulting modules.

        :param modules: A list of strings, containing redis modules 
                        eg: redisearch, redisai, etc.

        :param client_args: dictionary of dictionaries for modules in the module list
                            eg: {'redisearch': {'index_name': 'foo'}, ...}

        :param client: A pre-build client of type redis.Redis. This is used to
                       create all clients.
        """
        if client is not None:
            self.CLIENT = client
        else:
            self.CLIENT = Redis()

        for key, vals in modules.items():
            modclient = "{}_CLIENT".format(key.upper())
            x = importlib.import_module(key)
            setattr(self, modclient, x.Client(**vals))