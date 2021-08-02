from redis._compat import nativestr
from .utils import list_to_dict

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
        self.rules = response['rules']
        self.sourceKey = response['sourceKey']
        self.chunkCount = response['chunkCount']
        self.memory_usage = response['memoryUsage']
        self.total_samples = response['totalSamples']
        self.labels = list_to_dict(response['labels'])
        self.retention_msecs = response['retentionTime']
        self.lastTimeStamp = response['lastTimestamp']
        self.first_time_stamp = response['firstTimestamp']
        if 'maxSamplesPerChunk' in response:
            self.max_samples_per_chunk = response['maxSamplesPerChunk']
            self.chunk_size = self.max_samples_per_chunk * 16 # backward compatible changes
        if 'chunkSize' in response:
            self.chunk_size = response['chunkSize']
        if 'duplicatePolicy' in response:
            self.duplicate_policy = response['duplicatePolicy']
            if type(self.duplicate_policy) == bytes:
                self.duplicate_policy = self.duplicate_policy.decode()