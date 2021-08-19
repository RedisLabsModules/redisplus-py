CREATE_CMD = "TS.CREATE"
ALTER_CMD = "TS.ALTER"
ADD_CMD = "TS.ADD"
MADD_CMD = "TS.MADD"
INCRBY_CMD = "TS.INCRBY"
DECRBY_CMD = "TS.DECRBY"
DEL_CMD = "TS.DEL"
CREATERULE_CMD = "TS.CREATERULE"
DELETERULE_CMD = "TS.DELETERULE"
RANGE_CMD = "TS.RANGE"
REVRANGE_CMD = "TS.REVRANGE"
MRANGE_CMD = "TS.MRANGE"
MREVRANGE_CMD = "TS.MREVRANGE"
GET_CMD = "TS.GET"
MGET_CMD = "TS.MGET"
INFO_CMD = "TS.INFO"
QUERYINDEX_CMD = "TS.QUERYINDEX"


class CommandMixin:
    """RedisTimeSeries Commands."""

    def create(self, key, **kwargs):
        """
        Create a new time-series.

        Args:
            key: time-series key
            retention_msecs: Maximum age for samples compared to last event time (in milliseconds).
                        If None or 0 is passed then  the series is not trimmed at all.
            uncompressed: since RedisTimeSeries v1.2, both timestamps and values are compressed by default.
                        Adding this flag will keep data in an uncompressed form. Compression not only saves
                        memory but usually improve performance due to lower number of memory accesses
            labels: Set of label-value pairs that represent metadata labels of the key.
            chunk_size: Each time-serie uses chunks of memory of fixed size for time series samples.
                        You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes).
            duplicate_policy: since RedisTimeSeries v1.4 you can specify the duplicate sample policy ( Configure what to do on duplicate sample. )
                        Can be one of:
                              - 'block': an error will occur for any out of order sample
                              - 'first': ignore the new value
                              - 'last': override with latest value
                              - 'min': only override if the value is lower than the existing value
                              - 'max': only override if the value is higher than the existing value
                        When this is not set, the server-wide default will be used.
        """
        retention_msecs = kwargs.get("retention_msecs", None)
        uncompressed = kwargs.get("uncompressed", False)
        labels = kwargs.get("labels", {})
        chunk_size = kwargs.get("chunk_size", None)
        duplicate_policy = kwargs.get("duplicate_policy", None)
        params = [key]
        self.appendRetention(params, retention_msecs)
        self.appendUncompressed(params, uncompressed)
        self.appendChunkSize(params, chunk_size)
        self.appendDuplicatePolicy(params, CREATE_CMD, duplicate_policy)
        self.appendLabels(params, labels)

        return self.execute_command(CREATE_CMD, *params)

    def alter(self, key, **kwargs):
        """
        Update the retention, labels of an existing key.

        The parameters are the same as TS.CREATE.
        """
        retention_msecs = kwargs.get("retention_msecs", None)
        labels = kwargs.get("labels", {})
        duplicate_policy = kwargs.get("duplicate_policy", None)
        params = [key]
        self.appendRetention(params, retention_msecs)
        self.appendDuplicatePolicy(params, ALTER_CMD, duplicate_policy)
        self.appendLabels(params, labels)

        return self.execute_command(ALTER_CMD, *params)

    def add(self, key, timestamp, value, **kwargs):
        """
        Append (or create and append) a new sample to the series.

        Args:
            key: time-series key
            timestamp: timestamp of the sample. * can be used for automatic timestamp (using the system clock).
            value: numeric data value of the sample
            retention_msecs: Maximum age for samples compared to last event time (in milliseconds).
                        If None or 0 is passed then  the series is not trimmed at all.
            uncompressed: since RedisTimeSeries v1.2, both timestamps and values are compressed by default.
                        Adding this flag will keep data in an uncompressed form. Compression not only saves
                        memory but usually improve performance due to lower number of memory accesses
            labels: Set of label-value pairs that represent metadata labels of the key.
            chunk_size: Each time-serie uses chunks of memory of fixed size for time series samples.
                        You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes).
            duplicate_policy: since RedisTimeSeries v1.4 you can specify the duplicate sample policy ( Configure what to do on duplicate sample. )
                        Can be one of:
                              - 'block': an error will occur for any out of order sample
                              - 'first': ignore the new value
                              - 'last': override with latest value
                              - 'min': only override if the value is lower than the existing value
                              - 'max': only override if the value is higher than the existing value
                        When this is not set, the server-wide default will be used.
        """
        retention_msecs = kwargs.get("retention_msecs", None)
        uncompressed = kwargs.get("uncompressed", False)
        labels = kwargs.get("labels", {})
        chunk_size = kwargs.get("chunk_size", None)
        duplicate_policy = kwargs.get("duplicate_policy", None)
        params = [key, timestamp, value]
        self.appendRetention(params, retention_msecs)
        self.appendUncompressed(params, uncompressed)
        self.appendChunkSize(params, chunk_size)
        self.appendDuplicatePolicy(params, ADD_CMD, duplicate_policy)
        self.appendLabels(params, labels)

        return self.execute_command(ADD_CMD, *params)

    def madd(self, ktv_tuples):
        """
        Append (or create and append) a new ``value`` to series ``key`` with ``timestamp``.

        Expects a list of ``tuples`` as (``key``,``timestamp``, ``value``).
        Return value is an array with timestamps of insertions.
        """
        params = []
        for ktv in ktv_tuples:
            for item in ktv:
                params.append(item)

        return self.execute_command(MADD_CMD, *params)

    def incrby(self, key, value, **kwargs):
        """
        Increment (or create an time-series and increment) the latest sample's of a series.

        This command can be used as a counter or gauge that automatically gets history as a time series.

        Args:
            key: time-series key
            value: numeric data value of the sample
            timestamp: timestamp of the sample. None can be used for automatic timestamp (using the system clock).
            retention_msecs: Maximum age for samples compared to last event time (in milliseconds).
                        If None or 0 is passed then  the series is not trimmed at all.
            uncompressed: since RedisTimeSeries v1.2, both timestamps and values are compressed by default.
                        Adding this flag will keep data in an uncompressed form. Compression not only saves
                        memory but usually improve performance due to lower number of memory accesses
            labels: Set of label-value pairs that represent metadata labels of the key.
            chunk_size: Each time-series uses chunks of memory of fixed size for time series samples.
                        You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes).
        """
        timestamp = kwargs.get("timestamp", None)
        retention_msecs = kwargs.get("retention_msecs", None)
        uncompressed = kwargs.get("uncompressed", False)
        labels = kwargs.get("labels", {})
        chunk_size = kwargs.get("chunk_size", None)
        params = [key, value]
        self.appendTimestamp(params, timestamp)
        self.appendRetention(params, retention_msecs)
        self.appendUncompressed(params, uncompressed)
        self.appendChunkSize(params, chunk_size)
        self.appendLabels(params, labels)

        return self.execute_command(INCRBY_CMD, *params)

    def decrby(self, key, value, **kwargs):
        """
        Decrement (or create an time-series and decrement) the latest sample's of a series.

        This command can be used as a counter or gauge that automatically gets history as a time series.

        Args:
            key: time-series key
            value: numeric data value of the sample
            timestamp: timestamp of the sample. None can be used for automatic timestamp (using the system clock).
            retention_msecs: Maximum age for samples compared to last event time (in milliseconds).
                        If None or 0 is passed then  the series is not trimmed at all.
            uncompressed: since RedisTimeSeries v1.2, both timestamps and values are compressed by default.
                        Adding this flag will keep data in an uncompressed form. Compression not only saves
                        memory but usually improve performance due to lower number of memory accesses
            labels: Set of label-value pairs that represent metadata labels of the key.
            chunk_size: Each time-series uses chunks of memory of fixed size for time series samples.
                        You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes).
        """
        timestamp = kwargs.get("timestamp", None)
        retention_msecs = kwargs.get("retention_msecs", None)
        uncompressed = kwargs.get("uncompressed", False)
        labels = kwargs.get("labels", {})
        chunk_size = kwargs.get("chunk_size", None)
        params = [key, value]
        self.appendTimestamp(params, timestamp)
        self.appendRetention(params, retention_msecs)
        self.appendUncompressed(params, uncompressed)
        self.appendChunkSize(params, chunk_size)
        self.appendLabels(params, labels)

        return self.execute_command(DECRBY_CMD, *params)

    def delrange(self, key, from_time, to_time):
        """
        Delete data points for a given timeseries and interval range in the form of start and end delete timestamps.

        The given timestamp interval is closed (inclusive), meaning start and end data points will also be deleted.
        Return the count for deleted items.

        Args:
            key: time-series key.
            from_time: Start timestamp for the range deletion.
            to_time: End timestamp for the range deletion.
        """
        return self.execute_command(DEL_CMD, key, from_time, to_time)

    def createrule(self, source_key, dest_key, aggregation_type, bucket_size_msec):
        """
        Create a compaction rule from values added to ``source_key`` into ``dest_key``.

        Aggregating for ``bucket_size_msec`` where an ``aggregation_type`` can be
        ['avg', 'sum', 'min', 'max', 'range', 'count', 'first', 'last', 'std.p', 'std.s', 'var.p', 'var.s']
        """
        params = [source_key, dest_key]
        self.appendAggregation(params, aggregation_type, bucket_size_msec)

        return self.execute_command(CREATERULE_CMD, *params)

    def deleterule(self, source_key, dest_key):
        """Delete a compaction rule."""
        return self.execute_command(DELETERULE_CMD, source_key, dest_key)

    def __range_params(
        self,
        key,
        from_time,
        to_time,
        count,
        aggregation_type,
        bucket_size_msec,
        filter_by_ts,
        filter_by_min_value,
        filter_by_max_value,
        align,
    ):
        """Create TS.RANGE and TS.REVRANGE arguments."""
        params = [key, from_time, to_time]
        self.appendFilerByTs(params, filter_by_ts)
        self.appendFilerByValue(params, filter_by_min_value, filter_by_max_value)
        self.appendCount(params, count)
        self.appendAlign(params, align)
        self.appendAggregation(params, aggregation_type, bucket_size_msec)

        return params

    def range(
        self,
        key,
        from_time,
        to_time,
        count=None,
        aggregation_type=None,
        bucket_size_msec=0,
        filter_by_ts=None,
        filter_by_min_value=None,
        filter_by_max_value=None,
        align=None,
    ):
        """
        Query a range in forward direction for a specific time-serie.

        Args:
            key: Key name for timeseries.
            from_time: Start timestamp for the range query. - can be used to express the minimum possible timestamp (0).
            to_time:  End timestamp for range query, + can be used to express the maximum possible timestamp.
            count: Optional maximum number of returned results.
            aggregation_type: Optional aggregation type. Can be one of ['avg', 'sum', 'min', 'max', 'range', 'count',
            'first', 'last', 'std.p', 'std.s', 'var.p', 'var.s']
            bucket_size_msec: Time bucket for aggregation in milliseconds.
            filter_by_ts: List of timestamps to filter the result by specific timestamps.
            filter_by_min_value: Filter result by minimum value (must mention also filter_by_max_value).
            filter_by_max_value: Filter result by maximum value (must mention also filter_by_min_value).
            align: Timestamp for alignment control for aggregation.
        """
        params = self.__range_params(
            key,
            from_time,
            to_time,
            count,
            aggregation_type,
            bucket_size_msec,
            filter_by_ts,
            filter_by_min_value,
            filter_by_max_value,
            align,
        )
        return self.execute_command(RANGE_CMD, *params)

    def revrange(
        self,
        key,
        from_time,
        to_time,
        count=None,
        aggregation_type=None,
        bucket_size_msec=0,
        filter_by_ts=None,
        filter_by_min_value=None,
        filter_by_max_value=None,
        align=None,
    ):
        """
        Query a range in reverse direction for a specific time-series.

        Note: This command is only available since RedisTimeSeries >= v1.4

        Args:
            key: Key name for timeseries.
            from_time: Start timestamp for the range query. - can be used to express the minimum possible timestamp (0).
            to_time:  End timestamp for range query, + can be used to express the maximum possible timestamp.
            count: Optional maximum number of returned results.
            aggregation_type: Optional aggregation type. Can be one of ['avg', 'sum', 'min', 'max', 'range', 'count',
            'first', 'last', 'std.p', 'std.s', 'var.p', 'var.s']
            bucket_size_msec: Time bucket for aggregation in milliseconds.
            filter_by_ts: List of timestamps to filter the result by specific timestamps.
            filter_by_min_value: Filter result by minimum value (must mention also filter_by_max_value).
            filter_by_max_value: Filter result by maximum value (must mention also filter_by_min_value).
            align: Timestamp for alignment control for aggregation.
        """
        params = self.__range_params(
            key,
            from_time,
            to_time,
            count,
            aggregation_type,
            bucket_size_msec,
            filter_by_ts,
            filter_by_min_value,
            filter_by_max_value,
            align,
        )
        return self.execute_command(REVRANGE_CMD, *params)

    def mrange(
        self,
        from_time,
        to_time,
        filters,
        count=None,
        aggregation_type=None,
        bucket_size_msec=0,
        with_labels=False,
        filter_by_ts=None,
        filter_by_min_value=None,
        filter_by_max_value=None,
        groupby=None,
        reduce=None,
        select_labels=None,
        align=None,
    ):
        """
        Query a range across multiple time-series by filters in forward direction.

        Args:
            from_time: Start timestamp for the range query. - can be used to express the minimum possible timestamp (0).
            to_time:  End timestamp for range query, + can be used to express the maximum possible timestamp.
            filters: filter to match the time-series labels.
            count: Optional maximum number of returned results.
            aggregation_type: Optional aggregation type. Can be one of ['avg', 'sum', 'min', 'max', 'range', 'count',
            'first', 'last', 'std.p', 'std.s', 'var.p', 'var.s']
            bucket_size_msec: Time bucket for aggregation in milliseconds.
            with_labels:  Include in the reply the label-value pairs that represent metadata labels of the time-series.
            If this argument is not set, by default, an empty Array will be replied on the labels array position.
            filter_by_ts: List of timestamps to filter the result by specific timestamps.
            filter_by_min_value: Filter result by minimum value (must mention also filter_by_max_value).
            filter_by_max_value: Filter result by maximum value (must mention also filter_by_min_value).
            groupby: Grouping by fields the results (must mention also reduce).
            reduce: Applying reducer functions on each group. Can be one of ['sum', 'min', 'max'].
            select_labels: Include in the reply only a subset of the key-value pair labels of a series.
            align: Timestamp for alignment control for aggregation.
        """
        params = self.__mrange_params(
            aggregation_type,
            bucket_size_msec,
            count,
            filters,
            from_time,
            to_time,
            with_labels,
            filter_by_ts,
            filter_by_min_value,
            filter_by_max_value,
            groupby,
            reduce,
            select_labels,
            align,
        )

        return self.execute_command(MRANGE_CMD, *params)

    def __mrange_params(
        self,
        aggregation_type,
        bucket_size_msec,
        count,
        filters,
        from_time,
        to_time,
        with_labels,
        filter_by_ts,
        filter_by_min_value,
        filter_by_max_value,
        groupby,
        reduce,
        select_labels,
        align,
    ):
        """Create TS.MRANGE and TS.MREVRANGE arguments."""
        params = [from_time, to_time]
        self.appendFilerByTs(params, filter_by_ts)
        self.appendFilerByValue(params, filter_by_min_value, filter_by_max_value)
        self.appendCount(params, count)
        self.appendAlign(params, align)
        self.appendAggregation(params, aggregation_type, bucket_size_msec)
        self.appendWithLabels(params, with_labels, select_labels)
        params.extend(["FILTER"])
        params += filters
        self.appendGroupbyReduce(params, groupby, reduce)
        return params

    def mrevrange(
        self,
        from_time,
        to_time,
        filters,
        count=None,
        aggregation_type=None,
        bucket_size_msec=0,
        with_labels=False,
        filter_by_ts=None,
        filter_by_min_value=None,
        filter_by_max_value=None,
        groupby=None,
        reduce=None,
        select_labels=None,
        align=None,
    ):
        """
        Query a range across multiple time-series by filters in reverse direction.

        Args:
            from_time: Start timestamp for the range query. - can be used to express the minimum possible timestamp (0).
            to_time:  End timestamp for range query, + can be used to express the maximum possible timestamp.
            filters: filter to match the time-series labels.
            count: Optional maximum number of returned results.
            aggregation_type: Optional aggregation type. Can be one of ['avg', 'sum', 'min', 'max', 'range', 'count',
            'first', 'last', 'std.p', 'std.s', 'var.p', 'var.s']
            bucket_size_msec: Time bucket for aggregation in milliseconds.
            with_labels: Include in the reply the label-value pairs that represent metadata labels of the time-series.
            If this argument is not set, by default, an empty Array will be replied on the labels array position.
            filter_by_ts: List of timestamps to filter the result by specific timestamps.
            filter_by_min_value: Filter result by minimum value (must mention also filter_by_max_value).
            filter_by_max_value: Filter result by maximum value (must mention also filter_by_min_value).
            groupby: Grouping by fields the results (must mention also reduce).
            reduce: Applying reducer functions on each group. Can be one of ['sum', 'min', 'max'].
            select_labels: Include in the reply only a subset of the key-value pair labels of a series.
            align: Timestamp for alignment control for aggregation.
        """
        params = self.__mrange_params(
            aggregation_type,
            bucket_size_msec,
            count,
            filters,
            from_time,
            to_time,
            with_labels,
            filter_by_ts,
            filter_by_min_value,
            filter_by_max_value,
            groupby,
            reduce,
            select_labels,
            align,
        )

        return self.execute_command(MREVRANGE_CMD, *params)

    def get(self, key):
        """Get the last sample of ``key``."""
        return self.execute_command(GET_CMD, key)

    def mget(self, filters, with_labels=False):
        """Get the last samples matching the specific ``filter``."""
        params = []
        self.appendWithLabels(params, with_labels)
        params.extend(["FILTER"])
        params += filters
        return self.execute_command(MGET_CMD, *params)

    def info(self, key):
        """Get information of ``key``."""
        return self.execute_command(INFO_CMD, key)

    def queryindex(self, filters):
        """Get all the keys matching the ``filter`` list."""
        return self.execute_command(QUERYINDEX_CMD, *filters)
