from typing import Dict, Optional
from redis.client import Redis
import importlib


class RedisClient(object):
    """General client to be used for redis modules"""

    __redis_modules__ = []

    def __init__(self, modules: Dict, client=Optional[Redis], from_url=Optional[str]):
        """
        Creates an all-purpose client and passes them in to
        all of the resulting modules.

        :param modules: A dictionary of dictionaries for modules in the module
                        list.
                        eg: {'redisearch': {'index_name': 'foo'}, ...}
        :type modules: dict

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
            except ModuleNotFoundError:
                x = importlib.import_module("modules.%s" % key)
            except:
                raise AttributeError("No module {} found".format(key))
            vals['conn'] = self.CLIENT  # NOTE redisearch only, each module is different
            setattr(self, modclient, x.Client(**vals))
            self.__redis_modules__.append(x)

    @property
    def modules(self):
        return self.__redis_modules__

    @property
    def commands(self):
        key = '__commands__'
        commands = getattr(self, key, {})
        if commands != {}:
            return commands

        for r in self.modules:
            cmds = r.commands
            import inspect
            # print(r.commands.__dict__.values())
            print(inspect.getmembers(r.commands, inspect.isfunction))
        return "moose"
    #         commands.update(cmds)
    #     setattr(self, key, commands)
    #     return commands