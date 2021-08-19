from typing import Optional
import json

from redis.client import Redis

# from . import commands as recmds
from ..helpers import bulk_of_jsons, delist, nativestr
from .commands import CommandMixin
from ..feature import AbstractFeature


class JSON(CommandMixin, AbstractFeature, object):
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
            "JSON.GET": self._decode,
            "JSON.MGET": bulk_of_jsons(self._decode),
            "JSON.SET": lambda r: r and nativestr(r) == "OK",
            "JSON.NUMINCRBY": self._decode,
            "JSON.NUMMULTBY": self._decode,
            "JSON.TOGGLE": lambda b: b == b"true",
            "JSON.STRAPPEND": int,
            "JSON.STRLEN": int,
            "JSON.ARRAPPEND": int,
            "JSON.ARRINDEX": int,
            "JSON.ARRINSERT": int,
            "JSON.ARRLEN": int,
            "JSON.ARRPOP": self._decode,
            "JSON.ARRTRIM": int,
            "JSON.OBJLEN": int,
            "JSON.OBJKEYS": delist,
            # "JSON.RESP": delist,
            "JSON.DEBUG": int,
        }

        self.CLIENT = client

        for key, value in self.MODULE_CALLBACKS.items():
            self.client.set_response_callback(key, value)

        self.__encoder__ = encoder
        self.__decoder__ = decoder

        # # the encoding happens on the client object

    def _decode(self, obj):
        """Get the decoder."""
        if obj is None:
            return obj

        try:
            return self.__decoder__.decode(obj)
        except TypeError:
            return self.__decoder__.decode(obj.decode())

    def _encode(self, obj):
        """Get the encoder."""
        return self.__encoder__.encode(obj)

    def pipeline(self, **kwargs):
        p = self._pipeline(
            CommandMixin,
            __encode__=self.__encoder__,
            _encode=self._encode,
            __decoder__=self.__decoder__,
        )
        return p
