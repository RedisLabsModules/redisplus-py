from ..client import Client as TSClient
from ..utilsQuery import range_params, mrange_params

def range(client, key, from_time, to_time, count=None, aggregation_type=None,
          bucket_size_msec=0, filter_by_ts=None, filter_by_min_value=None,
          filter_by_max_value=None, align=None):
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
    params = range_params(key, from_time, to_time, count, aggregation_type, bucket_size_msec,
                                 filter_by_ts, filter_by_min_value, filter_by_max_value, align)
    return client.redis.execute_command(TSClient.RANGE_CMD, *params)

def revrange(client, key, from_time, to_time, count=None, aggregation_type=None,
             bucket_size_msec=0, filter_by_ts=None, filter_by_min_value=None,
             filter_by_max_value=None, align=None):
    """
    Query a range in reverse direction for a specific time-serie.
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
    params = range_params(key, from_time, to_time, count, aggregation_type, bucket_size_msec,
                                 filter_by_ts, filter_by_min_value, filter_by_max_value, align)
    return client.redis.execute_command(TSClient.REVRANGE_CMD, *params)

def mrange(client, from_time, to_time, filters, count=None, aggregation_type=None, bucket_size_msec=0,
           with_labels=False, filter_by_ts=None, filter_by_min_value=None, filter_by_max_value=None,
           groupby=None, reduce=None, select_labels=None, align=None):
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
    params = mrange_params(aggregation_type, bucket_size_msec, count, filters, from_time, to_time,
                           with_labels, filter_by_ts, filter_by_min_value, filter_by_max_value,
                           groupby, reduce, select_labels, align)

    return client.redis.execute_command(TSClient.MRANGE_CMD, *params)

def mrevrange(client, from_time, to_time, filters, count=None, aggregation_type=None, bucket_size_msec=0,
              with_labels=False, filter_by_ts=None, filter_by_min_value=None, filter_by_max_value=None,
              groupby=None, reduce=None, select_labels=None, align=None):
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
    params = mrange_params(aggregation_type, bucket_size_msec, count, filters, from_time, to_time,
                           with_labels, filter_by_ts, filter_by_min_value, filter_by_max_value,
                           groupby, reduce, select_labels, align)

    return client.redis.execute_command(TSClient.MREVRANGE_CMD, *params)

def get(client, key):
    """Gets the last sample of ``key``"""
    return client.redis.execute_command(TSClient.GET_CMD, key)

def mget(client, filters, with_labels=False):
    """Get the last samples matching the specific ``filter``."""
    params = []
    TSClient.appendWithLabels(params, with_labels)
    params.extend(['FILTER'])
    params += filters
    return client.redis.execute_command(TSClient.MGET_CMD, *params)
