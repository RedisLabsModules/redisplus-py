import functools
from redis import Redis
from redis.client import Pipeline, bool_ok
from redis.commands import Commands as RedisCommands

from .commands import CommandMixin
from .utils import *
from .info import *


class Client(CommandMixin, RedisCommands, object):  # changed from StrictRedis
    """
    This class subclasses redis-py's `Redis` and implements
    RedisBloom's commands.
    The client allows to interact with RedisBloom and use all of
    it's functionality.
    Prefix is according to the DS used.
    - BF for Bloom Filter
    - CF for Cuckoo Filter
    - CMS for Count-Min Sketch
    - TOPK for TopK Data Structure
    - TDIGEST for estimate rank statistics
    """

    BF_RESERVE = "BF.RESERVE"
    BF_ADD = "BF.ADD"
    BF_MADD = "BF.MADD"
    BF_INSERT = "BF.INSERT"
    BF_EXISTS = "BF.EXISTS"
    BF_MEXISTS = "BF.MEXISTS"
    BF_SCANDUMP = "BF.SCANDUMP"
    BF_LOADCHUNK = "BF.LOADCHUNK"
    BF_INFO = "BF.INFO"

    CF_RESERVE = "CF.RESERVE"
    CF_ADD = "CF.ADD"
    CF_ADDNX = "CF.ADDNX"
    CF_INSERT = "CF.INSERT"
    CF_INSERTNX = "CF.INSERTNX"
    CF_EXISTS = "CF.EXISTS"
    CF_DEL = "CF.DEL"
    CF_COUNT = "CF.COUNT"
    CF_SCANDUMP = "CF.SCANDUMP"
    CF_LOADCHUNK = "CF.LOADCHUNK"
    CF_INFO = "CF.INFO"

    CMS_INITBYDIM = "CMS.INITBYDIM"
    CMS_INITBYPROB = "CMS.INITBYPROB"
    CMS_INCRBY = "CMS.INCRBY"
    CMS_QUERY = "CMS.QUERY"
    CMS_MERGE = "CMS.MERGE"
    CMS_INFO = "CMS.INFO"

    TOPK_RESERVE = "TOPK.RESERVE"
    TOPK_ADD = "TOPK.ADD"
    TOPK_QUERY = "TOPK.QUERY"
    TOPK_COUNT = "TOPK.COUNT"
    TOPK_LIST = "TOPK.LIST"
    TOPK_INFO = "TOPK.INFO"

    TDIGEST_CREATE = "TDIGEST.CREATE"
    TDIGEST_RESET = "TDIGEST.RESET"
    TDIGEST_ADD = "TDIGEST.ADD"
    TDIGEST_MERGE = "TDIGEST.MERGE"
    TDIGEST_CDF = "TDIGEST.CDF"
    TDIGEST_QUANTILE = "TDIGEST.QUANTILE"
    TDIGEST_MIN = "TDIGEST.MIN"
    TDIGEST_MAX = "TDIGEST.MAX"
    TDIGEST_INFO = "TDIGEST.INFO"

    def __init__(self, client: Redis, *args, **kwargs):
        """
        Creates a new RedisBloom client.
        """
        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            self.BF_RESERVE: bool_ok,
            # self.BF_ADD: spaceHolder,
            # self.BF_MADD: spaceHolder,
            # self.BF_INSERT: spaceHolder,
            # self.BF_EXISTS: spaceHolder,
            # self.BF_MEXISTS: spaceHolder,
            # self.BF_SCANDUMP: spaceHolder,
            # self.BF_LOADCHUNK: spaceHolder,
            self.BF_INFO: BFInfo,
            self.CF_RESERVE: bool_ok,
            # self.CF_ADD: spaceHolder,
            # self.CF_ADDNX: spaceHolder,
            # self.CF_INSERT: spaceHolder,
            # self.CF_INSERTNX: spaceHolder,
            # self.CF_EXISTS: spaceHolder,
            # self.CF_DEL: spaceHolder,
            # self.CF_COUNT: spaceHolder,
            # self.CF_SCANDUMP: spaceHolder,
            # self.CF_LOADCHUNK: spaceHolder,
            self.CF_INFO: CFInfo,
            self.CMS_INITBYDIM: bool_ok,
            self.CMS_INITBYPROB: bool_ok,
            # self.CMS_INCRBY: spaceHolder,
            # self.CMS_QUERY: spaceHolder,
            self.CMS_MERGE: bool_ok,
            self.CMS_INFO: CMSInfo,
            self.TOPK_RESERVE: bool_ok,
            self.TOPK_ADD: parseToList,
            # self.TOPK_QUERY: spaceHolder,
            # self.TOPK_COUNT: spaceHolder,
            self.TOPK_LIST: parseToList,
            self.TOPK_INFO: TopKInfo,
            self.TDIGEST_CREATE: bool_ok,
            # self.TDIGEST_RESET: bool_ok,
            # self.TDIGEST_ADD: spaceHolder,
            # self.TDIGEST_MERGE: spaceHolder,
            self.TDIGEST_CDF: float,
            self.TDIGEST_QUANTILE: float,
            self.TDIGEST_MIN: float,
            self.TDIGEST_MAX: float,
            self.TDIGEST_INFO: TDigestInfo,
        }

        self.CLIENT = client
        self.client.pipeline = functools.partial(self.pipeline, self)

        for k, v in MODULE_CALLBACKS.items():
            self.client.set_response_callback(k, v)

    def execute_command(self, *args, **kwargs):
        return self.client.execute_command(*args, **kwargs)

    @property
    def client(self):
        return self.CLIENT

    @staticmethod
    def appendItems(params, items):
        params.extend(["ITEMS"])
        params += items

    @staticmethod
    def appendError(params, error):
        if error is not None:
            params.extend(["ERROR", error])

    @staticmethod
    def appendCapacity(params, capacity):
        if capacity is not None:
            params.extend(["CAPACITY", capacity])

    @staticmethod
    def appendExpansion(params, expansion):
        if expansion is not None:
            params.extend(["EXPANSION", expansion])

    @staticmethod
    def appendNoScale(params, noScale):
        if noScale is not None:
            params.extend(["NONSCALING"])

    @staticmethod
    def appendWeights(params, weights):
        if len(weights) > 0:
            params.append("WEIGHTS")
            params += weights

    @staticmethod
    def appendNoCreate(params, noCreate):
        if noCreate is not None:
            params.extend(["NOCREATE"])

    @staticmethod
    def appendItemsAndIncrements(params, items, increments):
        for i in range(len(items)):
            params.append(items[i])
            params.append(increments[i])

    @staticmethod
    def appendValuesAndWeights(params, items, weights):
        for i in range(len(items)):
            params.append(items[i])
            params.append(weights[i])

    @staticmethod
    def appendMaxIterations(params, max_iterations):
        if max_iterations is not None:
            params.extend(["MAXITERATIONS", max_iterations])

    @staticmethod
    def appendBucketSize(params, bucket_size):
        if bucket_size is not None:
            params.extend(["BUCKETSIZE", bucket_size])

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
            connection_pool=self.connection_pool,
            response_callbacks=self.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint,
        )
        return p


class Pipeline(Pipeline, Client):
    "Pipeline for RedisBloom Client"

    def __init__(self, connection_pool, response_callbacks, transaction, shard_hint):
        self.connection_pool = connection_pool
        self.connection = None
        self.response_callbacks = response_callbacks
        self.transaction = transaction
        self.shard_hint = shard_hint

        self.watching = False
        self.reset()

    def pipeline(self):
        raise AttributeError("Pipelines cannot be created within pipelines.")
