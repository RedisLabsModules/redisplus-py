"""RedisBloom commands."""


class CommandMixin:
    """RedisBloom commands."""

    # region Bloom Filter Functions
    def bfCreate(self, key, errorRate, capacity, expansion=None, noScale=None):
        """
        Create a new Bloom Filter ``key`` with desired probability of false positives ``errorRate`` expected entries to be inserted as ``capacity``.

        Default expansion value is 2. By default, filter is auto-scaling.
        """
        params = [key, errorRate, capacity]
        self.appendExpansion(params, expansion)
        self.appendNoScale(params, noScale)
        return self.execute_command(self.BF_RESERVE, *params)

    def bfAdd(self, key, item):
        """Add to a Bloom Filter ``key`` an ``item``."""
        params = [key, item]
        return self.execute_command(self.BF_ADD, *params)

    def bfMAdd(self, key, *items):
        """Add to a Bloom Filter ``key`` multiple ``items``."""
        params = [key]
        params += items
        return self.execute_command(self.BF_MADD, *params)

    def bfInsert(
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

        return self.execute_command(self.BF_INSERT, *params)

    def bfExists(self, key, item):
        """Check whether an ``item`` exists in Bloom Filter ``key``."""
        params = [key, item]
        return self.execute_command(self.BF_EXISTS, *params)

    def bfMExists(self, key, *items):
        """Check whether ``items`` exist in Bloom Filter ``key``."""
        params = [key]
        params += items
        return self.execute_command(self.BF_MEXISTS, *params)

    def bfScandump(self, key, iter):
        """
        Begin an incremental save of the bloom filter ``key``.

        This is useful for large bloom filters which cannot fit into the normal SAVE and RESTORE model.
        The first time this command is called, the value of ``iter`` should be 0.
        This command will return successive (iter, data) pairs until (0, NULL) to indicate completion.
        """
        params = [key, iter]
        return self.execute_command(self.BF_SCANDUMP, *params)

    def bfLoadChunk(self, key, iter, data):
        """
        Restore a filter previously saved using SCANDUMP.

        See the SCANDUMP command for example usage.
        This command will overwrite any bloom filter stored under key.
        Ensure that the bloom filter will not be modified between invocations.
        """
        params = [key, iter, data]
        return self.execute_command(self.BF_LOADCHUNK, *params)

    def bfInfo(self, key):
        """Return capacity, size, number of filters, number of items inserted, and expansion rate."""
        return self.execute_command(self.BF_INFO, key)

    # endregion

    # region Cuckoo Filter Functions
    def cfCreate(
        self, key, capacity, expansion=None, bucket_size=None, max_iterations=None
    ):
        """Create a new Cuckoo Filter ``key`` an initial ``capacity`` items."""
        params = [key, capacity]
        self.appendExpansion(params, expansion)
        self.appendBucketSize(params, bucket_size)
        self.appendMaxIterations(params, max_iterations)
        return self.execute_command(self.CF_RESERVE, *params)

    def cfAdd(self, key, item):
        """Add an ``item`` to a Cuckoo Filter ``key``."""
        params = [key, item]
        return self.execute_command(self.CF_ADD, *params)

    def cfAddNX(self, key, item):
        """
        Add an ``item`` to a Cuckoo Filter ``key`` only if item does not yet exist.

        Command might be slower that ``cfAdd``.
        """
        params = [key, item]
        return self.execute_command(self.CF_ADDNX, *params)

    def cfInsert(self, key, items, capacity=None, nocreate=None):
        """
        Add multiple ``items`` to a Cuckoo Filter ``key``, allowing the filter to be created with a custom ``capacity` if it does not yet exist.

        ``items`` must be provided as a list.
        """
        params = [key]
        self.appendCapacity(params, capacity)
        self.appendNoCreate(params, nocreate)
        self.appendItems(params, items)
        return self.execute_command(self.CF_INSERT, *params)

    def cfInsertNX(self, key, items, capacity=None, nocreate=None):
        """
        Add multiple ``items`` to a Cuckoo Filter ``key`` only if they do not exist yet, allowing the filter to be created with a custom ``capacity` if it does not yet exist.

        ``items`` must be provided as a list.
        """
        params = [key]
        self.appendCapacity(params, capacity)
        self.appendNoCreate(params, nocreate)
        self.appendItems(params, items)
        return self.execute_command(self.CF_INSERTNX, *params)

    def cfExists(self, key, item):
        """Check whether an ``item`` exists in Cuckoo Filter ``key``."""
        params = [key, item]
        return self.execute_command(self.CF_EXISTS, *params)

    def cfDel(self, key, item):
        """Delete ``item`` from ``key``."""
        params = [key, item]
        return self.execute_command(self.CF_DEL, *params)

    def cfCount(self, key, item):
        """Return the number of times an ``item`` may be in the ``key``."""
        params = [key, item]
        return self.execute_command(self.CF_COUNT, *params)

    def cfScandump(self, key, iter):
        """
        Begin an incremental save of the Cuckoo filter ``key``.

        This is useful for large Cuckoo filters which cannot fit into the normal
        SAVE and RESTORE model.
        The first time this command is called, the value of ``iter`` should be 0.
        This command will return successive (iter, data) pairs until
        (0, NULL) to indicate completion.
        """
        params = [key, iter]
        return self.execute_command(self.CF_SCANDUMP, *params)

    def cfLoadChunk(self, key, iter, data):
        """
        Restore a filter previously saved using SCANDUMP. See the SCANDUMP command for example usage.

        This command will overwrite any Cuckoo filter stored under key.
        Ensure that the Cuckoo filter will not be modified between invocations.
        """
        params = [key, iter, data]
        return self.execute_command(self.CF_LOADCHUNK, *params)

    def cfInfo(self, key):
        """Return size, number of buckets, number of filter, number of items inserted, number of items deleted, bucket size, expansion rate, and max iteration."""
        return self.execute_command(self.CF_INFO, key)

    # endregion

    # region Count-Min Sketch Functions
    def cmsInitByDim(self, key, width, depth):
        """Initialize a Count-Min Sketch ``key`` to dimensions (``width``, ``depth``) specified by user."""
        params = [key, width, depth]
        return self.execute_command(self.CMS_INITBYDIM, *params)

    def cmsInitByProb(self, key, error, probability):
        """Initialize a Count-Min Sketch ``key`` to characteristics (``error``, ``probability``) specified by user."""
        params = [key, error, probability]
        return self.execute_command(self.CMS_INITBYPROB, *params)

    def cmsIncrBy(self, key, items, increments):
        """
        Add/increase ``items`` to a Count-Min Sketch ``key`` by ''increments''.

        Both ``items`` and ``increments`` are lists.
        Example - cmsIncrBy('A', ['foo'], [1])
        """
        params = [key]
        self.appendItemsAndIncrements(params, items, increments)
        return self.execute_command(self.CMS_INCRBY, *params)

    def cmsQuery(self, key, *items):
        """Return count for an ``item`` from ``key``. Multiple items can be queried with one call."""
        params = [key]
        params += items
        return self.execute_command(self.CMS_QUERY, *params)

    def cmsMerge(self, destKey, numKeys, srcKeys, weights=[]):
        """
        Merge ``numKeys`` of sketches into ``destKey``. Sketches specified in ``srcKeys``.

        All sketches must have identical width and depth.
        ``Weights`` can be used to multiply certain sketches. Default weight is 1.
        Both ``srcKeys`` and ``weights`` are lists.
        """
        params = [destKey, numKeys]
        params += srcKeys
        self.appendWeights(params, weights)
        return self.execute_command(self.CMS_MERGE, *params)

    def cmsInfo(self, key):
        """Return width, depth and total count of the sketch."""
        return self.execute_command(self.CMS_INFO, key)

    # endregion

    # region Top-K Functions
    def topkReserve(self, key, k, width, depth, decay):
        """Create a new Cuckoo Filter ``key`` with desired probability of false positives ``errorRate`` expected entries to be inserted as ``size``."""
        params = [key, k, width, depth, decay]
        return self.execute_command(self.TOPK_RESERVE, *params)

    def topkAdd(self, key, *items):
        """Add one ``item`` or more to a Cuckoo Filter ``key``."""
        params = [key]
        params += items
        return self.execute_command(self.TOPK_ADD, *params)

    def topkQuery(self, key, *items):
        """Check whether one ``item`` or more is a Top-K item at ``key``."""
        params = [key]
        params += items
        return self.execute_command(self.TOPK_QUERY, *params)

    def topkCount(self, key, *items):
        """Return count for one ``item`` or more from ``key``."""
        params = [key]
        params += items
        return self.execute_command(self.TOPK_COUNT, *params)

    def topkList(self, key):
        """Return full list of items in Top-K list of ``key```."""
        return self.execute_command(self.TOPK_LIST, key)

    def topkInfo(self, key):
        """Return k, width, depth and decay values of ``key``."""
        return self.execute_command(self.TOPK_INFO, key)

    # endregion

    # region T-Digest Functions
    def tdigestCreate(self, key, compression):
        """Allocate the memory and initialize the t-digest."""
        params = [key, compression]
        return self.execute_command(self.TDIGEST_CREATE, *params)

    def tdigestReset(self, key):
        """Reset the sketch ``key`` to zero - empty out the sketch and re-initialize it."""
        return self.execute_command(self.TDIGEST_RESET, key)

    def tdigestAdd(self, key, values, weights):
        """
        Add one or more samples (value with weight) to a sketch ``key``.

        Both ``values`` and ``weights`` are lists.
        Example - tdigestAdd('A', [1500.0], [1.0])
        """
        params = [key]
        self.appendValuesAndWeights(params, values, weights)
        return self.execute_command(self.TDIGEST_ADD, *params)

    def tdigestMerge(self, toKey, fromKey):
        """Merge all of the values from 'fromKey' to 'toKey' sketch."""
        params = [toKey, fromKey]
        return self.execute_command(self.TDIGEST_MERGE, *params)

    def tdigestMin(self, key):
        """Return minimum value from the sketch ``key``. Will return DBL_MAX if the sketch is empty."""
        return self.execute_command(self.TDIGEST_MIN, key)

    def tdigestMax(self, key):
        """Return maximum value from the sketch ``key``. Will return DBL_MIN if the sketch is empty."""
        return self.execute_command(self.TDIGEST_MAX, key)

    def tdigestQuantile(self, key, quantile):
        """Return double value estimate of the cutoff such that a specified fraction of the data added to this TDigest would be less than or equal to the cutoff."""
        params = [key, quantile]
        return self.execute_command(self.TDIGEST_QUANTILE, *params)

    def tdigestCdf(self, key, value):
        """Return double fraction of all points added which are <= value."""
        params = [key, value]
        return self.execute_command(self.TDIGEST_CDF, *params)

    def tdigestInfo(self, key):
        """Return Compression, Capacity, Merged Nodes, Unmerged Nodes, Merged Weight, Unmerged Weight and Total Compressions."""
        return self.execute_command(self.TDIGEST_INFO, key)

    # endregion
