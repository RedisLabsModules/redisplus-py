import functools
from typing import Optional
import json

from redis.client import Pipeline, Redis

# from . import commands as recmds
from ..helpers import bulk_of_jsons, delist, nativestr
from .commands import CommandMixin
from ..feature import AbstractFeature


class Client(CommandMixin, AbstractFeature, object):
    """
    Create a client for talking to json.

    :param decoder:
    :type json.JSONDecoder: An instance of json.JSONDecoder

    :param encoder:
    :type json.JSONEncoder: An instance of json.JSONEncoder
    """

    def __init__(
        self,
        client: Redis,
        decoder: Optional[json.JSONDecoder] = json.JSONDecoder(),
        encoder: Optional[json.JSONEncoder] = json.JSONEncoder(),
    ):
        """
        Create a client for talking to json.

        :param decoder:
        :type json.JSONDecoder: An instance of json.JSONDecoder

        :param encoder:
        :type json.JSONEncoder: An instance of json.JSONEncoder
        """
        # Set the module commands' callbacks
        self.MODULE_CALLBACKS = {
            "JSON.CLEAR": int,
            "JSON.DEL": int,
            "JSON.FORGET": int,
            "JSON.GET": self.decode,
            "JSON.MGET": bulk_of_jsons(self.decode),
            "JSON.SET": lambda r: r and nativestr(r) == "OK",
            "JSON.NUMINCRBY": self.decode,
            "JSON.NUMMULTBY": self.decode,
            "JSON.TOGGLE": lambda b: b == b"true",
            "JSON.STRAPPEND": int,
            "JSON.STRLEN": int,
            "JSON.ARRAPPEND": int,
            "JSON.ARRINDEX": int,
            "JSON.ARRINSERT": int,
            "JSON.ARRLEN": int,
            "JSON.ARRPOP": self.decode,
            "JSON.ARRTRIM": int,
            "JSON.OBJLEN": int,
            "JSON.OBJKEYS": delist,
            # "JSON.RESP": delist,
            "JSON.DEBUG": int,
        }

        # super().__init__()
        self.CLIENT = client
        self.client.encode = encoder.encode
        self.client.pipeline = functools.partial(self.pipeline, Pipeline)

        for key, value in self.MODULE_CALLBACKS.items():
            self.client.set_response_callback(key, value)

        self.__encoder__ = encoder
        self.__decoder__ = decoder

        # # the encoding happens on the client object

    def decode(self, obj):
        """Get the decoder."""
        if obj is None:
            return obj

        try:
            return self.__decoder__.decode(obj)
        except TypeError:
            return self.__decoder__.decode(obj.decode())

    def encode(self, obj):
        """Get the encoder."""
        return self.__encoder__.encode(obj)

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for later execution.

        ``transaction`` indicates whether all commands should be executed atomically.
        Apart from making a group of operations atomic, pipelines are useful for reducing
        the back-and-forth overhead between the client and server.
        Overridden in order to provide the right client through the pipeline.
        """
        p = Pipeline(
            connection_pool=self.client.connection_pool,
            response_callbacks=self.client.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint,
        )
        p.__encoder__ = self.__encoder__
        p.__decoder__ = self.__decoder__
        return p


class Pipeline(Pipeline, Client):
    """Pipeline for client."""