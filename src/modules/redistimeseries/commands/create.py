from ..client import Client as TSClient

def create(client, key, **kwargs):
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
    retention_msecs = kwargs.get('retention_msecs', None)
    uncompressed = kwargs.get('uncompressed', False)
    labels = kwargs.get('labels', {})
    chunk_size = kwargs.get('chunk_size', None)
    duplicate_policy = kwargs.get('duplicate_policy', None)
    params = [key]
    TSClient.appendRetention(params, retention_msecs)
    TSClient.appendUncompressed(params, uncompressed)
    TSClient.appendChunkSize(params, chunk_size)
    TSClient.appendDuplicatePolicy(params, TSClient.CREATE_CMD, duplicate_policy)
    TSClient.appendLabels(params, labels)

    return client.redis.execute_command(TSClient.CREATE_CMD, *params)
