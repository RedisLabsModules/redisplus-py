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
TOPK_INCRBY = "TOPK.INCRBY"
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


class BFCommandMixin:
    """RedisBloom commands."""

    # region Bloom Filter Functions
    def create(self, key, errorRate, capacity, expansion=None, noScale=None):
        """
        Create a new Bloom Filter ``key`` with desired probability of false positives ``errorRate`` expected entries to be inserted as ``capacity``.

        Default expansion value is 2. By default, filter is auto-scaling.
        """
        params = [key, errorRate, capacity]
        self.appendExpansion(params, expansion)
        self.appendNoScale(params, noScale)
        return self.execute_command(BF_RESERVE, *params)

    def add(self, key, item):
        """Add to a Bloom Filter ``key`` an ``item``."""
        params = [key, item]
        return self.execute_command(BF_ADD, *params)

    def madd(self, key, *items):
        """Add to a Bloom Filter ``key`` multiple ``items``."""
        params = [key]
        params += items
        return self.execute_command(BF_MADD, *params)

    def insert(
        self,
        key,
        items,
        capacity=None,
        error=None,
        noCreate=None,
        expansion=None,
        noScale=None,
    ):
        """
        Add to a Bloom Filter ``key`` multiple ``items``.

        If ``nocreate`` remain ``None`` and ``key does not exist, a new Bloom Filter
        ``key`` will be created with desired probability of false positives ``errorRate``
        and expected entries to be inserted as ``size``.
        """
        params = [key]
        self.appendCapacity(params, capacity)
        self.appendError(params, error)
        self.appendExpansion(params, expansion)
        self.appendNoCreate(params, noCreate)
        self.appendNoScale(params, noScale)
        self.appendItems(params, items)

        return self.execute_command(BF_INSERT, *params)

    def exists(self, key, item):
        """Check whether an ``item`` exists in Bloom Filter ``key``."""
        params = [key, item]
        return self.execute_command(BF_EXISTS, *params)

    def mexists(self, key, *items):
        """Check whether ``items`` exist in Bloom Filter ``key``."""
        params = [key]
        params += items
        return self.execute_command(BF_MEXISTS, *params)

    def scandump(self, key, iter):
        """
        Begin an incremental save of the bloom filter ``key``.

        This is useful for large bloom filters which cannot fit into the normal SAVE and RESTORE model.
        The first time this command is called, the value of ``iter`` should be 0.
        This command will return successive (iter, data) pairs until (0, NULL) to indicate completion.
        """
        params = [key, iter]
        return self.execute_command(BF_SCANDUMP, *params)

    def loadchunk(self, key, iter, data):
        """
        Restore a filter previously saved using SCANDUMP.

        See the SCANDUMP command for example usage.
        This command will overwrite any bloom filter stored under key.
        Ensure that the bloom filter will not be modified between invocations.
        """
        params = [key, iter, data]
        return self.execute_command(BF_LOADCHUNK, *params)

    def info(self, key):
        """Return capacity, size, number of filters, number of items inserted, and expansion rate."""
        return self.execute_command(BF_INFO, key)


class CFCommandMixin:

    # region Cuckoo Filter Functions
    def create(
        self, key, capacity, expansion=None, bucket_size=None, max_iterations=None
    ):
        """Create a new Cuckoo Filter ``key`` an initial ``capacity`` items."""
        params = [key, capacity]
        self.appendExpansion(params, expansion)
        self.appendBucketSize(params, bucket_size)
        self.appendMaxIterations(params, max_iterations)
        return self.execute_command(CF_RESERVE, *params)

    def add(self, key, item):
        """Add an ``item`` to a Cuckoo Filter ``key``."""
        params = [key, item]
        return self.execute_command(CF_ADD, *params)

    def addnx(self, key, item):
        """
        Add an ``item`` to a Cuckoo Filter ``key`` only if item does not yet exist.

        Command might be slower that ``cfAdd``.
        """
        params = [key, item]
        return self.execute_command(CF_ADDNX, *params)

    def insert(self, key, items, capacity=None, nocreate=None):
        """
        Add multiple ``items`` to a Cuckoo Filter ``key``, allowing the filter to be created with a custom ``capacity` if it does not yet exist.

        ``items`` must be provided as a list.
        """
        params = [key]
        self.appendCapacity(params, capacity)
        self.appendNoCreate(params, nocreate)
        self.appendItems(params, items)
        return self.execute_command(CF_INSERT, *params)

    def insertnx(self, key, items, capacity=None, nocreate=None):
        """
        Add multiple ``items`` to a Cuckoo Filter ``key`` only if they do not exist yet, allowing the filter to be created with a custom ``capacity` if it does not yet exist.

        ``items`` must be provided as a list.
        """
        params = [key]
        self.appendCapacity(params, capacity)
        self.appendNoCreate(params, nocreate)
        self.appendItems(params, items)
        return self.execute_command(CF_INSERTNX, *params)

    def exists(self, key, item):
        """Check whether an ``item`` exists in Cuckoo Filter ``key``."""
        params = [key, item]
        return self.execute_command(CF_EXISTS, *params)

    def delete(self, key, item):
        """Delete ``item`` from ``key``."""
        params = [key, item]
        return self.execute_command(CF_DEL, *params)

    def count(self, key, item):
        """Return the number of times an ``item`` may be in the ``key``."""
        params = [key, item]
        return self.execute_command(CF_COUNT, *params)

    def scandump(self, key, iter):
        """
        Begin an incremental save of the Cuckoo filter ``key``.

        This is useful for large Cuckoo filters which cannot fit into the normal
        SAVE and RESTORE model.
        The first time this command is called, the value of ``iter`` should be 0.
        This command will return successive (iter, data) pairs until
        (0, NULL) to indicate completion.
        """
        params = [key, iter]
        return self.execute_command(CF_SCANDUMP, *params)

    def loadchunk(self, key, iter, data):
        """
        Restore a filter previously saved using SCANDUMP. See the SCANDUMP command for example usage.

        This command will overwrite any Cuckoo filter stored under key.
        Ensure that the Cuckoo filter will not be modified between invocations.
        """
        params = [key, iter, data]
        return self.execute_command(CF_LOADCHUNK, *params)

    def info(self, key):
        """Return size, number of buckets, number of filter, number of items inserted, number of items deleted, bucket size, expansion rate, and max iteration."""
        return self.execute_command(CF_INFO, key)


class TOPKCommandMixin:
    def reserve(self, key, k, width, depth, decay):
        """
        Create a new Top-K Filter ``key`` with desired probability of false
        positives ``errorRate`` expected entries to be inserted as ``size``.
        """
        params = [key, k, width, depth, decay]
        return self.execute_command(TOPK_RESERVE, *params)

    def add(self, key, *items):
        """Add one ``item`` or more to a Top-K Filter ``key``."""
        params = [key]
        params += items
        return self.execute_command(TOPK_ADD, *params)

    def incrby(self, key, items, increments):
        """
        Add/increase ``items`` to a Top-K Sketch ``key`` by ''increments''.

        Both ``items`` and ``increments`` are lists.
        Example - topkincrby('A', ['foo'], [1])
        """
        params = [key]
        self.appendItemsAndIncrements(params, items, increments)
        return self.execute_command(TOPK_INCRBY, *params)

    def query(self, key, *items):
        """Check whether one ``item`` or more is a Top-K item at ``key``."""
        params = [key]
        params += items
        return self.execute_command(TOPK_QUERY, *params)

    def count(self, key, *items):
        """Return count for one ``item`` or more from ``key``."""
        params = [key]
        params += items
        return self.execute_command(TOPK_COUNT, *params)

    def list(self, key, withcount=False):
        """
        Return full list of items in Top-K list of ``key```.
        If `withcount` set to True, return full list of items
        with probabilistic count in Top-K list of ``key```.
        """
        params = [key]
        if withcount:
            params.append("WITHCOUNT")
        return self.execute_command(TOPK_LIST, *params)

    def info(self, key):
        """Return k, width, depth and decay values of ``key``."""
        return self.execute_command(TOPK_INFO, key)


class TDigestCommandMixin:
    def create(self, key, compression):
        """Allocate the memory and initialize the t-digest."""
        params = [key, compression]
        return self.execute_command(TDIGEST_CREATE, *params)

    def reset(self, key):
        """Reset the sketch ``key`` to zero - empty out the sketch and re-initialize it."""
        return self.execute_command(TDIGEST_RESET, key)

    def add(self, key, values, weights):
        """
        Add one or more samples (value with weight) to a sketch ``key``.

        Both ``values`` and ``weights`` are lists.
        Example - tdigestadd('A', [1500.0], [1.0])
        """
        params = [key]
        self.appendValuesAndWeights(params, values, weights)
        return self.execute_command(TDIGEST_ADD, *params)

    def merge(self, toKey, fromKey):
        """Merge all of the values from 'fromKey' to 'toKey' sketch."""
        params = [toKey, fromKey]
        return self.execute_command(TDIGEST_MERGE, *params)

    def min(self, key):
        """Return minimum value from the sketch ``key``. Will return DBL_MAX if the sketch is empty."""
        return self.execute_command(TDIGEST_MIN, key)

    def max(self, key):
        """Return maximum value from the sketch ``key``. Will return DBL_MIN if the sketch is empty."""
        return self.execute_command(TDIGEST_MAX, key)

    def quantile(self, key, quantile):
        """Return double value estimate of the cutoff such that a specified fraction of the data added to this TDigest would be less than or equal to the cutoff."""
        params = [key, quantile]
        return self.execute_command(TDIGEST_QUANTILE, *params)

    def cdf(self, key, value):
        """Return double fraction of all points added which are <= value."""
        params = [key, value]
        return self.execute_command(TDIGEST_CDF, *params)

    def info(self, key):
        """Return Compression, Capacity, Merged Nodes, Unmerged Nodes, Merged Weight, Unmerged Weight and Total Compressions."""
        return self.execute_command(TDIGEST_INFO, key)


class CMSCommandMixin:

    # region Count-Min Sketch Functions
    def initbydim(self, key, width, depth):
        """Initialize a Count-Min Sketch ``key`` to dimensions (``width``, ``depth``) specified by user."""
        params = [key, width, depth]
        return self.execute_command(CMS_INITBYDIM, *params)

    def initbyprob(self, key, error, probability):
        """Initialize a Count-Min Sketch ``key`` to characteristics (``error``, ``probability``) specified by user."""
        params = [key, error, probability]
        return self.execute_command(CMS_INITBYPROB, *params)

    def incrby(self, key, items, increments):
        """
        Add/increase ``items`` to a Count-Min Sketch ``key`` by ''increments''.

        Both ``items`` and ``increments`` are lists.
        Example - cmsincrby('A', ['foo'], [1])
        """
        params = [key]
        self.appendItemsAndIncrements(params, items, increments)
        return self.execute_command(CMS_INCRBY, *params)

    def query(self, key, *items):
        """Return count for an ``item`` from ``key``. Multiple items can be queried with one call."""
        params = [key]
        params += items
        return self.execute_command(CMS_QUERY, *params)

    def merge(self, destKey, numKeys, srcKeys, weights=[]):
        """
        Merge ``numKeys`` of sketches into ``destKey``. Sketches specified in ``srcKeys``.

        All sketches must have identical width and depth.
        ``Weights`` can be used to multiply certain sketches. Default weight is 1.
        Both ``srcKeys`` and ``weights`` are lists.
        """
        params = [destKey, numKeys]
        params += srcKeys
        self.appendWeights(params, weights)
        return self.execute_command(CMS_MERGE, *params)

    def info(self, key):
        """Return width, depth and total count of the sketch."""
        return self.execute_command(CMS_INFO, key)
