from ..client import Client as TSClient


def alter(client, key, **kwargs):
    """
    Update the retention, labels of an existing key. The parameters
    are the same as TS.CREATE.
    """
    retention_msecs = kwargs.get('retention_msecs', None)
    labels = kwargs.get('labels', {})
    duplicate_policy = kwargs.get('duplicate_policy', None)
    params = [key]
    TSClient.appendRetention(params, retention_msecs)
    TSClient.appendDuplicatePolicy(params, TSClient.ALTER_CMD, duplicate_policy)
    TSClient.appendLabels(params, labels)

    return client.redis.execute_command(TSClient.ALTER_CMD, *params)

def add(client, key, timestamp, value, **kwargs):
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
    retention_msecs = kwargs.get('retention_msecs', None)
    uncompressed = kwargs.get('uncompressed', False)
    labels = kwargs.get('labels', {})
    chunk_size = kwargs.get('chunk_size', None)
    duplicate_policy = kwargs.get('duplicate_policy', None)
    params = [key, timestamp, value]
    TSClient.appendRetention(params, retention_msecs)
    TSClient.appendUncompressed(params, uncompressed)
    TSClient.appendChunkSize(params, chunk_size)
    TSClient.appendDuplicatePolicy(params, TSClient.ADD_CMD, duplicate_policy)
    TSClient.appendLabels(params, labels)

    return client.redis.execute_command(TSClient.ADD_CMD, *params)

def madd(client, ktv_tuples):
    """
    Appends (or creates and appends) a new ``value`` to series
    ``key`` with ``timestamp``. Expects a list of ``tuples`` as
    (``key``,``timestamp``, ``value``). Return value is an
    array with timestamps of insertions.
    """
    params = []
    for ktv in ktv_tuples:
        for item in ktv:
            params.append(item)

    return client.redis.execute_command(TSClient.MADD_CMD, *params)

def incrby(client, key, value, **kwargs):
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
    timestamp = kwargs.get('timestamp', None)
    retention_msecs = kwargs.get('retention_msecs', None)
    uncompressed = kwargs.get('uncompressed', False)
    labels = kwargs.get('labels', {})
    chunk_size = kwargs.get('chunk_size', None)
    params = [key, value]
    TSClient.appendTimestamp(params, timestamp)
    TSClient.appendRetention(params, retention_msecs)
    TSClient.appendUncompressed(params, uncompressed)
    TSClient.appendChunkSize(params, chunk_size)
    TSClient.appendLabels(params, labels)

    return client.redis.execute_command(TSClient.INCRBY_CMD, *params)

def decrby(client, key, value, **kwargs):
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
        chunk_size: Each time-serie uses chunks of memory of fixed size for time series samples.
                    You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes).
    """
    timestamp = kwargs.get('timestamp', None)
    retention_msecs = kwargs.get('retention_msecs', None)
    uncompressed = kwargs.get('uncompressed', False)
    labels = kwargs.get('labels', {})
    chunk_size = kwargs.get('chunk_size', None)
    params = [key, value]
    TSClient.appendTimestamp(params, timestamp)
    TSClient.appendRetention(params, retention_msecs)
    TSClient.appendUncompressed(params, uncompressed)
    TSClient.appendChunkSize(params, chunk_size)
    TSClient.appendLabels(params, labels)

    return client.redis.execute_command(TSClient.DECRBY_CMD, *params)

def delrange(client, key, from_time, to_time):
    """
    Delete data points for a given timeseries and interval range in the form of start and end delete timestamps.
    The given timestamp interval is closed (inclusive), meaning start and end data points will also be deleted.
    Return the count for deleted items.

    Args:
        key: time-series key.
        from_time: Start timestamp for the range deletion.
        to_time: End timestamp for the range deletion.
    """
    return client.redis.execute_command(TSClient.DEL_CMD, key, from_time, to_time)
