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
            DEL_CMD: bool_ok,
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

        self.CLIENT = client

        for k in MODULE_CALLBACKS:
            self.client.set_response_callback(k, MODULE_CALLBACKS[k])

    @staticmethod
    def appendUncompressed(params, uncompressed):
        """Append UNCOMPRESSED tag to params."""
        if uncompressed:
            params.extend(["UNCOMPRESSED"])

    @staticmethod
    def appendWithLabels(params, with_labels, select_labels=None):
        """Append labels behavior to params."""
        if with_labels and select_labels:
            raise DataError(
                "with_labels and select_labels cannot be provided together."
            )

        if with_labels:
            params.extend(["WITHLABELS"])
        if select_labels:
            params.extend(["SELECTED_LABELS", *select_labels])

    @staticmethod
    def appendGroupbyReduce(params, groupby, reduce):
        """Append GROUPBY REDUCE property to params."""
        if groupby is not None and reduce is not None:
            params.extend(["GROUPBY", groupby, "REDUCE", reduce.upper()])

    @staticmethod
    def appendRetention(params, retention):
        """Append RETENTION property to params."""
        if retention is not None:
            params.extend(["RETENTION", retention])

    @staticmethod
    def appendLabels(params, labels):
        """Append LABELS property to params."""
        if labels:
            params.append("LABELS")
            for k, v in labels.items():
                params.extend([k, v])

    @staticmethod
    def appendCount(params, count):
        """Append COUNT property to params."""
        if count is not None:
            params.extend(["COUNT", count])

    @staticmethod
    def appendTimestamp(params, timestamp):
        """Append TIMESTAMP property to params."""
        if timestamp is not None:
            params.extend(["TIMESTAMP", timestamp])

    @staticmethod
    def appendAlign(params, align):
        """Append ALIGN property to params."""
        if align is not None:
            params.extend(["ALIGN", align])

    @staticmethod
    def appendAggregation(params, aggregation_type, bucket_size_msec):
        """Append AGGREGATION property to params."""
        if aggregation_type is not None:
            params.append("AGGREGATION")
            params.extend([aggregation_type, bucket_size_msec])

    @staticmethod
    def appendChunkSize(params, chunk_size):
        """Append CHUNK_SIZE property to params."""
        if chunk_size is not None:
            params.extend(["CHUNK_SIZE", chunk_size])

    @staticmethod
    def appendDuplicatePolicy(params, command, duplicate_policy):
        """Append DUPLICATE_POLICY property to params on CREATE and ON_DUPLICATE on ADD."""
        if duplicate_policy is not None:
            if command == "TS.ADD":
                params.extend(["ON_DUPLICATE", duplicate_policy])
            else:
                params.extend(["DUPLICATE_POLICY", duplicate_policy])

    @staticmethod
    def appendFilerByTs(params, ts_list):
        """Append FILTER_BY_TS property to params."""
        if ts_list is not None:
            params.extend(["FILTER_BY_TS", *ts_list])

    @staticmethod
    def appendFilerByValue(params, min_value, max_value):
        """Append FILTER_BY_VALUE property to params."""
        if min_value is not None and max_value is not None:
            params.extend(["FILTER_BY_VALUE", min_value, max_value])

    def pipeline(self, **kwargs):
        p = self._pipeline(
            CommandMixin,
        )
        return p
