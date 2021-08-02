from ..client import Client as TSClient

def createrule(client, source_key, dest_key,
               aggregation_type, bucket_size_msec):
    """
    Creates a compaction rule from values added to ``source_key``
    into ``dest_key``. Aggregating for ``bucket_size_msec`` where an
    ``aggregation_type`` can be ['avg', 'sum', 'min', 'max',
    'range', 'count', 'first', 'last', 'std.p', 'std.s', 'var.p', 'var.s']
    """
    params = [source_key, dest_key]
    TSClient.appendAggregation(params, aggregation_type, bucket_size_msec)

    return client.redis.execute_command(TSClient.CREATERULE_CMD, *params)

def deleterule(client, source_key, dest_key):
    """
    Deletes a compaction rule
    """
    return client.redis.execute_command(TSClient.DELETERULE_CMD, source_key, dest_key)
