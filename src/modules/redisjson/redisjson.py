# from src.modules import redisjson
from redis.client import Redis

# import importlib
# import inspect
from . import commands as recmds

from redisplus.modules import ModuleClient


class Client(ModuleClient):
    def __init__(self, conn: Redis):
        """
        Creates a client for talking to redisjson.

        :param client: A pre-build client of type redis.Redis. This is used to
                       execute all commands.
        """

        if conn is None:
            raise AttributeError("No redis client provided.")

        self.CLIENT = conn
        super().__init__()
        from .commands import jsonstrlen

    def _execute_command(self, **kwargs):
        return self.CLIENT.execute_command(**kwargs)

    commands = recmds
