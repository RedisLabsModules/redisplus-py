import functools
import importlib
import inspect
from typing import Dict, Optional
from redis.client import Redis


class RedisClient(object):
    """General client to be used for redis modules"""

    # modules that properly instantiated
    __redis_modules__ = {}

    # list of active commands
    __commands__ = []

    def __init__(
        self,
        modules: Dict,
        safe_load: Optional[bool]=False,
        client: Optional[Redis]=None,
        from_url: Optional[str]=None,
    ):
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
            # modclient = "{}_CLIENT".format(key.upper())
            try:
                x = importlib.import_module(key)
            except (ModuleNotFoundError, ImportError):
                x = importlib.import_module("modules.%s" % key)
            except:
                if safe_load is False:
                    raise AttributeError("No module {} found".format(key))

            vals["conn"] = self.CLIENT  # NOTE redisearch only, each module is different
            self.__redis_modules__[key] = x.Client(**vals)
            self._load_module_commands()

    def _load_module_commands(self):
        """Tries to load commands from modules into the client
        namespace, where possible."""

        _commands = self.commands
        for mod, client in self.__redis_modules__.items():
            try:
                modcmds = client.commands
            except AttributeError:
                continue

            # now, for each module, get all of the commands listed in the
            # command package, setting them on this class object
            for obj in inspect.getmembers(modcmds, inspect.isfunction):
                if obj[1].__module__.find("commands") != -1:

                    # re-wrap the function so that it now takes all objects
                    # other than a client - since we'll pass those in.
                    part_cmd_wrapper = functools.partial(obj[1], client)
                    setattr(self.CLIENT, obj[0], part_cmd_wrapper)
                    _commands.append(obj[0])

        self.__commands__ = list(set(_commands))

    @property
    def client(self):
        """Returns the redis client, used for this connection."""
        return self.CLIENT

    @property
    def modules(self):
        """Returns the list of configured modules"""
        return list(self.__redis_modules__.keys())

    @property
    def commands(self):
        """Accessor for the list of supported commands"""
        return self.__commands__