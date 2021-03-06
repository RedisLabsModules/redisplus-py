from redis.client import bool_ok
from ..feature import AbstractFeature

from .commands import *  # lgtm [py/polluting-import]
from ..helpers import parseToList
from .info import (
    BFInfo,
    CFInfo,
    CMSInfo,
    TopKInfo,
    TDigestInfo,
)


class AbstractBloom(AbstractFeature, object):
    @staticmethod
    def appendItems(params, items):
        """Append ITEMS to params."""
        params.extend(["ITEMS"])
        params += items

    @staticmethod
    def appendError(params, error):
        """Append ERROR to params."""
        if error is not None:
            params.extend(["ERROR", error])

    @staticmethod
    def appendCapacity(params, capacity):
        """Append CAPACITY to params."""
        if capacity is not None:
            params.extend(["CAPACITY", capacity])

    @staticmethod
    def appendExpansion(params, expansion):
        """Append EXPANSION to params."""
        if expansion is not None:
            params.extend(["EXPANSION", expansion])

    @staticmethod
    def appendNoScale(params, noScale):
        """Append NONSCALING tag to params."""
        if noScale is not None:
            params.extend(["NONSCALING"])

    @staticmethod
    def appendWeights(params, weights):
        """Append WEIGHTS to params."""
        if len(weights) > 0:
            params.append("WEIGHTS")
            params += weights

    @staticmethod
    def appendNoCreate(params, noCreate):
        """Append NOCREATE tag to params."""
        if noCreate is not None:
            params.extend(["NOCREATE"])

    @staticmethod
    def appendItemsAndIncrements(params, items, increments):
        """Append pairs of items and increments to params."""
        for i in range(len(items)):
            params.append(items[i])
            params.append(increments[i])

    @staticmethod
    def appendValuesAndWeights(params, items, weights):
        """Append pairs of items and weights to params."""
        for i in range(len(items)):
            params.append(items[i])
            params.append(weights[i])

    @staticmethod
    def appendMaxIterations(params, max_iterations):
        """Append MAXITERATIONS to params."""
        if max_iterations is not None:
            params.extend(["MAXITERATIONS", max_iterations])

    @staticmethod
    def appendBucketSize(params, bucket_size):
        """Append BUCKETSIZE to params."""
        if bucket_size is not None:
            params.extend(["BUCKETSIZE", bucket_size])


class CMSBloom(CMSCommandMixin, AbstractBloom):
    def __init__(self, client, **kwargs):
        """Create a new RedisBloom client."""
        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            CMS_INITBYDIM: bool_ok,
            CMS_INITBYPROB: bool_ok,
            # CMS_INCRBY: spaceHolder,
            # CMS_QUERY: spaceHolder,
            CMS_MERGE: bool_ok,
            CMS_INFO: CMSInfo,
        }

        self.client = client
        self.commandmixin = CMSCommandMixin

        for k, v in MODULE_CALLBACKS.items():
            self.client.set_response_callback(k, v)


class TOPKBloom(TOPKCommandMixin, AbstractBloom):
    def __init__(self, client, **kwargs):
        """Create a new RedisBloom client."""
        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            TOPK_RESERVE: bool_ok,
            TOPK_ADD: parseToList,
            TOPK_INCRBY: parseToList,
            # TOPK_QUERY: spaceHolder,
            # TOPK_COUNT: spaceHolder,
            TOPK_LIST: parseToList,
            TOPK_INFO: TopKInfo,
        }

        self.client = client
        self.commandmixin = TOPKCommandMixin

        for k, v in MODULE_CALLBACKS.items():
            self.client.set_response_callback(k, v)


class CFBloom(CFCommandMixin, AbstractBloom):
    def __init__(self, client, **kwargs):
        """Create a new RedisBloom client."""
        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            CF_RESERVE: bool_ok,
            # CF_ADD: spaceHolder,
            # CF_ADDNX: spaceHolder,
            # CF_INSERT: spaceHolder,
            # CF_INSERTNX: spaceHolder,
            # CF_EXISTS: spaceHolder,
            # CF_DEL: spaceHolder,
            # CF_COUNT: spaceHolder,
            # CF_SCANDUMP: spaceHolder,
            # CF_LOADCHUNK: spaceHolder,
            CF_INFO: CFInfo,
        }

        self.client = client
        self.commandmixin = CFCommandMixin

        for k, v in MODULE_CALLBACKS.items():
            self.client.set_response_callback(k, v)


class TDigestBloom(TDigestCommandMixin, AbstractBloom):
    def __init__(self, client, **kwargs):
        """Create a new RedisBloom client."""
        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            TDIGEST_CREATE: bool_ok,
            # TDIGEST_RESET: bool_ok,
            # TDIGEST_ADD: spaceHolder,
            # TDIGEST_MERGE: spaceHolder,
            TDIGEST_CDF: float,
            TDIGEST_QUANTILE: float,
            TDIGEST_MIN: float,
            TDIGEST_MAX: float,
            TDIGEST_INFO: TDigestInfo,
        }

        self.client = client
        self.commandmixin = TDigestCommandMixin

        for k, v in MODULE_CALLBACKS.items():
            self.client.set_response_callback(k, v)


class BFBloom(BFCommandMixin, AbstractBloom):
    """
    This class subclasses redis-py's `Redis` and implements RedisBloom's commands.

    The client allows to interact with RedisBloom and use all of
    it's functionality.
    Prefix is according to the DS used.
    - BF for Bloom Filter
    - CF for Cuckoo Filter
    - CMS for Count-Min Sketch
    - TOPK for TopK Data Structure
    - TDIGEST for estimate rank statistics
    """

    def __init__(self, client, **kwargs):
        """Create a new RedisBloom client."""
        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            BF_RESERVE: bool_ok,
            # BF_ADD: spaceHolder,
            # BF_MADD: spaceHolder,
            # BF_INSERT: spaceHolder,
            # BF_EXISTS: spaceHolder,
            # BF_MEXISTS: spaceHolder,
            # BF_SCANDUMP: spaceHolder,
            # BF_LOADCHUNK: spaceHolder,
            BF_INFO: BFInfo,
        }

        self.client = client
        self.commandmixin = BFCommandMixin

        for k, v in MODULE_CALLBACKS.items():
            self.client.set_response_callback(k, v)
