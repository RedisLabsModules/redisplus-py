from .client import Client as TSClient

def range_params(key, from_time, to_time, count, aggregation_type, bucket_size_msec,
                 filter_by_ts, filter_by_min_value, filter_by_max_value, align):
    """
    Internal method to create TS.RANGE and TS.REVRANGE arguments
    """
    params = [key, from_time, to_time]
    TSClient.appendFilerByTs(params, filter_by_ts)
    TSClient.appendFilerByValue(params, filter_by_min_value, filter_by_max_value)
    TSClient.appendCount(params, count)
    TSClient.appendAlign(params, align)
    TSClient.appendAggregation(params, aggregation_type, bucket_size_msec)

    return params

def mrange_params(aggregation_type, bucket_size_msec, count, filters, from_time, to_time,
                  with_labels, filter_by_ts, filter_by_min_value, filter_by_max_value, groupby,
                  reduce, select_labels, align):
    """
    Internal method to create TS.MRANGE and TS.MREVRANGE arguments
    """
    params = [from_time, to_time]
    TSClient.appendFilerByTs(params, filter_by_ts)
    TSClient.appendFilerByValue(params, filter_by_min_value, filter_by_max_value)
    TSClient.appendCount(params, count)
    TSClient.appendAlign(params, align)
    TSClient.appendAggregation(params, aggregation_type, bucket_size_msec)
    TSClient.appendWithLabels(params, with_labels, select_labels)
    params.extend(['FILTER'])
    params += filters
    TSClient.appendGroupbyReduce(params, groupby, reduce)

    return params
