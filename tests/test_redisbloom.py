import pytest
from redis import Redis
from redisplus.client import RedisClient

@pytest.fixture
def client():
    rc = RedisClient(modules={'redisbloom': {"client": Redis()}})
    rc.redisbloom.flushdb()

    return rc.redisbloom

@pytest.mark.redisbloom
def test_base(client):
    rc = RedisClient(modules={'redisbloom': {"client": Redis()}})
    rc.redisbloom.flushdb()

def testCreate(client):
    '''Test CREATE/RESERVE calls'''
    assert (client.bfCreate('bloom', 0.01, 1000))
    assert (client.bfCreate('bloom_e', 0.01, 1000, expansion=1))
    assert (client.bfCreate('bloom_ns', 0.01, 1000, noScale=True))
    assert (client.cfCreate('cuckoo', 1000))
    assert (client.cfCreate('cuckoo_e', 1000, expansion=1))
    assert (client.cfCreate('cuckoo_bs', 1000, bucket_size=4))
    assert (client.cfCreate('cuckoo_mi', 1000, max_iterations=10))
    assert (client.cmsInitByDim('cmsDim', 100, 5))
    assert (client.cmsInitByProb('cmsProb', 0.01, 0.01))
    assert (client.topkReserve('topk', 5, 100, 5, 0.9))
    assert (client.tdigestCreate('tDigest', 100))
