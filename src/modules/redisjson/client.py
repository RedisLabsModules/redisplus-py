from typing import Optional
import json

from redis._compat import long, nativestr
from redis.client import Redis
from redisplus.modules import ModuleClient

from . import commands as recmds
from .utils import bulk_of_jsons


class Client(ModuleClient):
    def __init__(
        self,
        conn: Redis,
        decoder: Optional[json.JSONDecoder] = json.JSONDecoder(),
        encoder: Optional[json.JSONEncoder] = json.JSONEncoder(),
    ):
        """
        Creates a client for talking to redisjson.

        :param client: A pre-build client of type redis.Redis. This is used to
                       execute all commands.
        :type redis.Redis: A redis-py compatible client

        :param decoder:
        :type json.JSONDecoder: An instance of json.JSONDecoder

        :param encoder:
        :type json.JSONEncoder: An instance of json.JSONEncoder
        """

        if conn is None:
            raise AttributeError("No redis client provided.")

        self.CLIENT = conn

        # Set the module commands' callbacks
        self.MODULE_CALLBACKS = {
            "JSON.DEL": long,
            "JSON.GET": self.decode,
            "JSON.MGET": bulk_of_jsons(self.decode),
            "JSON.SET": lambda r: r and nativestr(r) == "OK",
            "JSON.NUMINCRBY": self.decode,
            "JSON.NUMMULTBY": self.decode,
            "JSON.STRAPPEND": long,
            "JSON.STRLEN": long,
            "JSON.ARRAPPEND": long,
            "JSON.ARRINDEX": long,
            "JSON.ARRINSERT": long,
            "JSON.ARRLEN": long,
            "JSON.ARRPOP": self.decode,
            "JSON.ARRTRIM": long,
            "JSON.OBJLEN": long,
        }

        self.__encoder__ = encoder
        self.__decoder__ = decoder
        super().__init__()

    def execute_command(self, *args, **kwargs):
        return self.CLIENT.execute_command(*args, **kwargs)

    def decode(self, obj):
        return self.__decoder__.decode(obj)

    def encode(self, obj):
        return self.__encoder__.encode(obj)

    commands = recmds
