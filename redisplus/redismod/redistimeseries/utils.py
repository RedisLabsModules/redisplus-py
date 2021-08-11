from ..utils import nativestr


class TSInfo(object):
    rules = []
    labels = []
    sourceKey = None
    chunk_count = None
    memory_usage = None
    total_samples = None
    retention_msecs = None
    last_time_stamp = None
    first_time_stamp = None
    # As of RedisTimeseries >= v1.4 max_samples_per_chunk is deprecated in favor of chunk_size
    max_samples_per_chunk = None
    chunk_size = None
    duplicate_policy = None

    def __init__(self, args):
        response = dict(zip(map(nativestr, args[::2]), args[1::2]))
        self.rules = response["rules"]
        self.sourceKey = response["sourceKey"]
        self.chunkCount = response["chunkCount"]
        self.memory_usage = response["memoryUsage"]
        self.total_samples = response["totalSamples"]
        self.labels = list_to_dict(response["labels"])
        self.retention_msecs = response["retentionTime"]
        self.lastTimeStamp = response["lastTimestamp"]
        self.first_time_stamp = response["firstTimestamp"]
        if "maxSamplesPerChunk" in response:
            self.max_samples_per_chunk = response["maxSamplesPerChunk"]
            self.chunk_size = (
                    self.max_samples_per_chunk * 16
            )  # backward compatible changes
        if "chunkSize" in response:
            self.chunk_size = response["chunkSize"]
        if "duplicatePolicy" in response:
            self.duplicate_policy = response["duplicatePolicy"]
            if type(self.duplicate_policy) == bytes:
                self.duplicate_policy = self.duplicate_policy.decode()


def list_to_dict(aList):
    return {nativestr(aList[i][0]): nativestr(aList[i][1]) for i in range(len(aList))}


def parse_range(response):
    """Parse range response. Used by TS.RANGE and TS.REVRANGE."""
    return [tuple((r[0], float(r[1]))) for r in response]


def parse_m_range(response):
    """Parse multi range response. Used by TS.MRANGE and TS.MREVRANGE."""
    res = []
    for item in response:
        res.append({nativestr(item[0]): [list_to_dict(item[1]), parse_range(item[2])]})
    return sorted(res, key=lambda d: list(d.keys()))


def parse_get(response):
    """Parse get response. Used by TS.GET."""
    if response == []:
        return None
    return (int(response[0]), float(response[1]))


def parse_m_get(response):
    """Parse multi get response. Used by TS.MGET."""
    res = []
    for item in response:
        if item[2] == []:
            res.append({nativestr(item[0]): [list_to_dict(item[1]), None, None]})
        else:
            res.append(
                {
                    nativestr(item[0]): [
                        list_to_dict(item[1]),
                        int(item[2][0]),
                        float(item[2][1]),
                    ]
                }
            )
    return sorted(res, key=lambda d: list(d.keys()))


def parseToList(response):
    """Parse the response to list. Used by TS.QUERYINDEX."""
    res = []
    for item in response:
        res.append(nativestr(item))
    return res
