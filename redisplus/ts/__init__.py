from redis import Redis, DataError
from redis.client import bool_ok
from ..feature import AbstractFeature

from .utils import (
    parse_range,
    parse_get,
    parse_m_range,
    parse_m_get,
)
from .info import TSInfo
from ..helpers import parseToList
from .commands import *


class TimeSeries(CommandMixin, AbstractFeature, object):
    """
    This class subclasses redis-py's `Redis` and implements RedisTimeSeries's commands (prefixed with "ts").

    The client allows to interact with RedisTimeSeries and use all of it's functionality.
    """

    def __init__(self, client=None, **kwargs):
        """Create a new RedisTimeSeries client."""

        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            CREATE_CMD: bool_ok,
            ALTER_CMD: bool_ok,
            CREATERULE_CMD: bool_ok,
            DEL_CMD: int,
            DELETERULE_CMD: bool_ok,
            RANGE_CMD: parse_range,
            REVRANGE_CMD: parse_range,
            MRANGE_CMD: parse_m_range,
            MREVRANGE_CMD: parse_m_range,
            GET_CMD: parse_get,
            MGET_CMD: parse_m_get,
            INFO_CMD: TSInfo,
            QUERYINDEX_CMD: parseToList,
        }

        self.client = client
        self.commandmixin = CommandMixin

        for k in MODULE_CALLBACKS:
            self.client.set_response_callback(k, MODULE_CALLBACKS[k])
