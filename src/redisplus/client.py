from typing import Dict, Optional
from redis.client import Redis
import importlib
import inspect


class RedisClient(object):
    """General client to be used for redis modules"""

    __redis_modules__ = []
    __commands__ = []

    def __init__(self, modules: Dict, safe_load=False, client=Optional[Redis], from_url=Optional[str]):
        """
        Creates an all-purpose client and passes them in to
        all of the resulting modules.

        :param modules: A dictionary of dictionaries for modules in the module
                        list.
                        eg: {'redisearch': {'index_name': 'foo'}, ...}
        :type modules: dict

        :param safe_load: Safely load modules, continuing if one fails to load
        :type safe_load: bool

        :param client: A pre-build client of type redis.Redis. This is used to
                       create all clients.

        :param from_url: A connection string, for building a redis client.
        :type from_url: str
        """
        if client is not None:
            self.CLIENT = client
        else:
            if from_url is not None:
                self.CLIENT = Redis.from_url(from_url)
            else:
                self.CLIENT = Redis()

        for key, vals in modules.items():
            modclient = "{}_CLIENT".format(key.upper())
            try:
                x = importlib.import_module(key)
            except (ModuleNotFoundError, ImportError):
                x = importlib.import_module("modules.%s" % key)
            except:
                if safe_load is False:
                    raise AttributeError("No module {} found".format(key))

            vals['conn'] = self.CLIENT  # NOTE redisearch only, each module is different
            setattr(self, modclient, x.Client(**vals))
            self.__redis_modules__.append(x)
            self._load_module_commands()

    def _load_module_commands(self):
        _commands = self.commands
        for r in self.modules:
            try:
                modcmds = r.commands
            except AttributeError:
                continue

            for obj in inspect.getmembers(modcmds, inspect.isfunction):
                if obj[1].__module__.find('commands') != -1:
                    _commands.append(obj[0])
                    setattr(self, obj[0], obj[1])
        self.__commands__ = list(set(_commands))

    @property
    def modules(self):
        """Returns the list of configured modules"""
        return self.__redis_modules__

    @property
    def commands(self):
        """Accessor for the list of supported commands"""
        return self.__commands__