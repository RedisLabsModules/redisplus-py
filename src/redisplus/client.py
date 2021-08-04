import functools
import importlib
import inspect
from typing import Dict, Optional
from redis.client import Redis


class RedisClient(object):
    """General client to be used for redis modules"""

    # modules that properly instantiated
    __redis_modules__ = []

    # list of active commands
    __commands__ = []

    def __init__(self,
        modules: Dict={},
        client: Optional[Redis]=None,
        safe_load=False,
    ):
        """
        :param modules: A list of dictionaries for modules to configure and
            their values.
            eg: {'redisearch': {'index_name: 'foo'}, ...}
        :type modules: dist
        """
        self.CLIENT = client
        self.__module__init__ = modules

        for key in modules.keys():
            self._initclient(key, safe_load)
            # setattr(self, key.upper(), x.Client(**vals))
            # self.__redis_modules__.append(key)
            # self._load_module_commands(key, self.__redis_modules__[key])

    def _initclient(self, module: str, safe_load=False):
        mod = self.__module__init__.get(module, None)
        if mod is None:
            raise AttributeError("{} is not a valid module." % module)

        try:
            x = importlib.import_module("modules.%s" % module)
        except:
            if safe_load is False:
                raise AttributeError("No module {} found".format(module))
            else:
                return

        con = mod.get("client", self.client)
        if con is None:
            raise AttributeError("Either a redis client must be passed ",
                                "into the class definition, or as an ",
                                "object on the dictionary."
                )
        mod.update({"client": con})
        setattr(self, module.upper(), x.Client(**mod))
        if module not in self.__redis_modules__:
            self.__redis_modules__.append(module)

    refresh=_initclient

    @property
    def client(self):
        """Returns the redis client, used for this connection."""
        return self.CLIENT

    @property
    def modules(self):
        """Returns the list of configured modules"""
        return self.__redis_modules__