from redis import Redis, DataError
import functools
from redis.client import Pipeline, bool_ok
from redis.commands import Commands as RedisCommands

from ..utils import nativestr
from .utils import *
from .commands import CommandMixin


class Client(CommandMixin, RedisCommands, object):  # changed from StrictRedis
    """
    This class subclasses redis-py's `Redis` and implements
    RedisTimeSeries's commands (prefixed with "ts").
    The client allows to interact with RedisTimeSeries and use all of
    it's functionality.
    """

    CREATE_CMD = 'TS.CREATE'
    ALTER_CMD = 'TS.ALTER'
    ADD_CMD = 'TS.ADD'
    MADD_CMD = 'TS.MADD'
    INCRBY_CMD = 'TS.INCRBY'
    DECRBY_CMD = 'TS.DECRBY'
    DEL_CMD = 'TS.DEL'
    CREATERULE_CMD = 'TS.CREATERULE'
    DELETERULE_CMD = 'TS.DELETERULE'
    RANGE_CMD = 'TS.RANGE'
    REVRANGE_CMD = 'TS.REVRANGE'
    MRANGE_CMD = 'TS.MRANGE'
    MREVRANGE_CMD = 'TS.MREVRANGE'
    GET_CMD = 'TS.GET'
    MGET_CMD = 'TS.MGET'
    INFO_CMD = 'TS.INFO'
    QUERYINDEX_CMD = 'TS.QUERYINDEX'

    def __init__(self, client=None, *args, **kwargs):
        """
        Creates a new RedisTimeSeries client.
        """
        self.redis = client if client is not None else Redis(*args, **kwargs)

        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            self.CREATE_CMD: bool_ok,
            self.ALTER_CMD: bool_ok,
            self.CREATERULE_CMD: bool_ok,
            self.DELETERULE_CMD: bool_ok,
            self.RANGE_CMD: parse_range,
            self.REVRANGE_CMD: parse_range,
            self.MRANGE_CMD: parse_m_range,
            self.MREVRANGE_CMD: parse_m_range,
            self.GET_CMD: parse_get,
            self.MGET_CMD: parse_m_get,
            self.INFO_CMD: TSInfo,
            self.QUERYINDEX_CMD: parseToList,
        }


        self.CLIENT = client
        self.client.pipeline = functools.partial(self.pipeline, self)

        for k in MODULE_CALLBACKS:
            self.redis.set_response_callback(k, MODULE_CALLBACKS[k])

    def execute_command(self, *args, **kwargs):
        return self.client.execute_command(*args, **kwargs)

    @property
    def client(self):
        return self.CLIENT

    @staticmethod
    def appendUncompressed(params, uncompressed):
        if uncompressed:
            params.extend(['UNCOMPRESSED'])

    @staticmethod
    def appendWithLabels(params, with_labels, select_labels=None):
        if with_labels and select_labels:
            raise DataError("with_labels and select_labels cannot be provided together.")

        if with_labels:
            params.extend(['WITHLABELS'])
        if select_labels:
            params.extend(['SELECTED_LABELS', *select_labels])

    @staticmethod
    def appendGroupbyReduce(params, groupby, reduce):
        if groupby is not None and reduce is not None:
            params.extend(['GROUPBY', groupby, 'REDUCE', reduce.upper()])

    @staticmethod
    def appendRetention(params, retention):
        if retention is not None:
            params.extend(['RETENTION', retention])

    @staticmethod
    def appendLabels(params, labels):
        if labels:
            params.append('LABELS')
            for k, v in labels.items():
                params.extend([k, v])

    @staticmethod
    def appendCount(params, count):
        if count is not None:
            params.extend(['COUNT', count])

    @staticmethod
    def appendTimestamp(params, timestamp):
        if timestamp is not None:
            params.extend(['TIMESTAMP', timestamp])

    @staticmethod
    def appendAlign(params, align):
        if align is not None:
            params.extend(['ALIGN', align])

    @staticmethod
    def appendAggregation(params, aggregation_type,
                          bucket_size_msec):
        params.append('AGGREGATION')
        params.extend([aggregation_type, bucket_size_msec])

    @staticmethod
    def appendChunkSize(params, chunk_size):
        if chunk_size is not None:
            params.extend(['CHUNK_SIZE', chunk_size])

    @staticmethod
    def appendDuplicatePolicy(params, command, duplicate_policy):
        if duplicate_policy is not None:
            if command == 'TS.ADD':
                params.extend(['ON_DUPLICATE', duplicate_policy])
            else:
                params.extend(['DUPLICATE_POLICY', duplicate_policy])

    @staticmethod
    def appendFilerByTs(params, ts_list):
        if ts_list is not None:
            params.extend(["FILTER_BY_TS", *ts_list])

    @staticmethod
    def appendFilerByValue(params, min_value, max_value):
        if min_value is not None and max_value is not None:
            params.extend(["FILTER_BY_VALUE", min_value, max_value])

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        Overridden in order to provide the right client through the pipeline.
        """
        p = Pipeline(
            connection_pool=self.redis.connection_pool,
            response_callbacks=self.redis.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint)
        p.redis = p
        return p


class Pipeline(Pipeline, Client):
    "Pipeline for Redis TimeSeries Client"
