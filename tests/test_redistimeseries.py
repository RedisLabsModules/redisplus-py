import pytest
import time
from time import sleep
from redis import Redis
from redisplus.client import RedisClient

@pytest.fixture
def client():
    global version
    rc = RedisClient(modules={'redistimeseries': {"client": Redis()}})
    rc.redistimeseries.flushdb()
    modules = rc.redistimeseries.execute_command("module", "list")
    if modules is not None:
        for module_info in modules:
            if module_info[1] == b'timeseries':
                version = int(module_info[3])
    return rc.redistimeseries

@pytest.mark.redistimeseries
def test_base(client):

    # base load
    rc = RedisClient(client=Redis())
    rc.client.flushdb()

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testVersionRuntime(client):
    import src.redisplus.redismod.redistimeseries as rts_pkg
    assert "" != rts_pkg.__version__

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testCreate(client):
    assert client.create(1)
    assert client.create(2, retention_msecs=5)
    assert client.create(3, labels={'Redis':'Labs'})
    assert client.create(4, retention_msecs=20, labels={'Time':'Series'})
    info = client.info(4)
    assert 20 == info.retention_msecs
    assert 'Series' == info.labels['Time']

    if version is None or version < 14000:
        return

    # Test for a chunk size of 128 Bytes
    assert client.create("time-serie-1",chunk_size=128)
    info = client.info("time-serie-1")
    assert 128, info.chunk_size

    # Test for duplicate policy
    for duplicate_policy in ["block","last","first","min","max"]:
        ts_name = "time-serie-ooo-{0}".format(duplicate_policy)
        assert client.create(ts_name, duplicate_policy=duplicate_policy)
        info = client.info(ts_name)
        assert duplicate_policy == info.duplicate_policy

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testAlter(client):
    assert client.create(1)
    assert 0 == client.info(1).retention_msecs
    assert client.alter(1, retention_msecs=10)
    assert {} == client.info(1).labels
    assert 10, client.info(1).retention_msecs
    assert client.alter(1, labels={'Time': 'Series'})
    assert 'Series' == client.info(1).labels['Time']
    assert 10 == client.info(1).retention_msecs
    pipe = client.pipeline()
    assert (pipe.create(2))

    if version is None or version < 14000:
        return
    info = client.info(1)
    assert info.duplicate_policy is None
    assert (client.alter(1, duplicate_policy='min'))
    info = client.info(1)
    assert 'min' == info.duplicate_policy

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testAdd(client):
    assert 1 == client.add(1, 1, 1)
    assert 2 == client.add(2, 2, 3, retention_msecs=10)
    assert 3 == client.add(3, 3, 2, labels={'Redis':'Labs'})
    assert 4 == client.add(4, 4, 2, retention_msecs=10, labels={'Redis': 'Labs', 'Time': 'Series'})
    assert round(time.time()) == round(float(client.add(5, '*', 1)) / 1000)

    info = client.info(4)
    assert 10 == info.retention_msecs
    assert 'Labs' == info.labels['Redis']

    if version is None or version < 14000:
        return

    # Test for a chunk size of 128 Bytes on TS.ADD
    assert client.add("time-serie-1", 1, 10.0, chunk_size=128)
    info = client.info("time-serie-1")
    assert 128 == info.chunk_size

    # Test for duplicate policy BLOCK
    assert 1 == client.add("time-serie-add-ooo-block", 1, 5.0)
    try:
        client.add("time-serie-add-ooo-block", 1, 5.0, duplicate_policy='block')
    except Exception as e:
        assert "TSDB: Error at upsert, update is not supported in BLOCK mode" == e.__str__()

    # Test for duplicate policy LAST
    assert 1 == client.add("time-serie-add-ooo-last", 1, 5.0)
    assert 1 == client.add("time-serie-add-ooo-last", 1, 10.0, duplicate_policy='last')
    assert 10.0 == client.get("time-serie-add-ooo-last")[1]

    # Test for duplicate policy FIRST
    assert 1 == client.add("time-serie-add-ooo-first", 1, 5.0)
    assert 1 == client.add("time-serie-add-ooo-first", 1, 10.0, duplicate_policy='first')
    assert 5.0 == client.get("time-serie-add-ooo-first")[1]

    # Test for duplicate policy MAX
    assert 1 == client.add("time-serie-add-ooo-max", 1, 5.0)
    assert 1 == client.add("time-serie-add-ooo-max", 1, 10.0, duplicate_policy='max')
    assert 10.0 == client.get("time-serie-add-ooo-max")[1]

    # Test for duplicate policy MIN
    assert 1 == client.add("time-serie-add-ooo-min", 1, 5.0)
    assert 1 == client.add("time-serie-add-ooo-min", 1, 10.0, duplicate_policy='min')
    assert 5.0 == client.get("time-serie-add-ooo-min")[1]

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testMAdd(client):
    client.create('a')
    assert [1, 2, 3] == client.madd([('a', 1, 5), ('a', 2, 10), ('a', 3, 15)])

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testIncrbyDecrby(client):
    for _ in range(100):
        assert client.incrby(1, 1)
        sleep(0.001)
    assert 100 == client.get(1)[1]
    for _ in range(100):
        assert client.decrby(1, 1)
        sleep(0.001)
    assert 0 == client.get(1)[1]

    assert client.incrby(2, 1.5, timestamp=5)
    assert (5, 1.5) == client.get(2)
    assert client.incrby(2, 2.25, timestamp=7)
    assert (7, 3.75) == client.get(2)
    assert client.decrby(2, 1.5, timestamp=15)
    assert (15, 2.25) == client.get(2)
    if version is None or version < 14000:
        return

    # Test for a chunk size of 128 Bytes on TS.INCRBY
    assert client.incrby("time-serie-1", 10, chunk_size=128)
    info = client.info("time-serie-1")
    assert 128 == info.chunk_size

    # Test for a chunk size of 128 Bytes on TS.DECRBY
    assert client.decrby("time-serie-2", 10, chunk_size=128)
    info = client.info("time-serie-2")
    assert 128 == info.chunk_size

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testCreateAndDeleteRule(client):
    # test rule creation
    time = 100
    client.create(1)
    client.create(2)
    client.createrule(1, 2, 'avg', 100)
    for i in range(50):
        client.add(1, time + i * 2, 1)
        client.add(1, time + i * 2 + 1, 2)
    client.add(1, time * 2, 1.5)
    assert round(client.get(2)[1], 5) == 1.5
    info = client.info(1)
    assert info.rules[0][1] == 100

    # test rule deletion
    client.deleterule(1, 2)
    info = client.info(1)
    assert not info.rules

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testDelRange(client):
    # TS.DEL is available since RedisTimeSeries >= v1.4
    if version is None or version < 14000:
        return

    try:
        client.delrange('test', 0, 100)
    except Exception as e:
        assert e.__str__() is not ""

    for i in range(100):
        client.add(1, i, i % 7)
    assert (22 == client.delrange(1, 0, 21))
    assert ([] == client.range(1, 0, 21))
    assert ([(22, 1.0)] == client.range(1, 22, 22))

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testRange(client):
    for i in range(100):
        client.add(1, i, i % 7)
    assert 100 == len(client.range(1, 0, 200))
    for i in range(100):
        client.add(1, i + 200, i % 7)
    assert (200 == len(client.range(1, 0, 500)))
    # last sample isn't returned
    assert (20 == len(client.range(1, 0, 500, aggregation_type='avg', bucket_size_msec=10)))
    assert (10 == len(client.range(1, 0, 500, count=10)))

    # available since RedisTimeSeries >= v1.4
    if version is None or version < 14000:
        return
    
    assert (2 == len(client.range(1, 0, 500, filter_by_ts=[i for i in range(10, 20)],
                                  filter_by_min_value=1, filter_by_max_value=2)))
    assert ([(0, 10.0), (10, 1.0)] ==
                     client.range(1, 0, 10, aggregation_type='count', bucket_size_msec=10, align='+'))
    assert ([(-5, 5.0), (5, 6.0)] ==
                     client.range(1, 0, 10, aggregation_type='count', bucket_size_msec=10, align=5))

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testRevRange(client):
    # TS.REVRANGE is available since RedisTimeSeries >= v1.4
    if version is None or version < 14000:
        return

    for i in range(100):
        client.add(1, i, i % 7)
    assert (100 == len(client.range(1, 0, 200)))
    for i in range(100):
        client.add(1, i + 200, i % 7)
    assert (200 == len(client.range(1, 0, 500)))
    # first sample isn't returned
    assert (20 == len(client.revrange(1, 0, 500, aggregation_type='avg', bucket_size_msec=10)))
    assert (10 == len(client.revrange(1, 0, 500, count=10)))
    assert (2 == len(client.revrange(1, 0, 500, filter_by_ts=[i for i in range(10, 20)], filter_by_min_value=1,
                                     filter_by_max_value=2)))
    assert ([(10, 1.0), (0, 10.0)] ==
                     client.revrange(1, 0, 10, aggregation_type='count', bucket_size_msec=10, align='+'))
    assert ([(1, 10.0), (-9, 1.0)] ==
                     client.revrange(1, 0, 10, aggregation_type='count', bucket_size_msec=10, align=1))

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testMultiRange(client):
    client.create(1, labels={'Test': 'This', 'team': 'ny'})
    client.create(2, labels={'Test': 'This', 'Taste': 'That', 'team': 'sf'})
    for i in range(100):
        client.add(1, i, i % 7)
        client.add(2, i, i % 11)
    
    res = client.mrange(0, 200, filters=['Test=This'])
    assert 2 == len(res)
    assert 100 == len(res[0]['1'][1])
    
    res = client.mrange(0, 200, filters=['Test=This'], count=10)
    assert 10 == len(res[0]['1'][1])
    
    for i in range(100):
        client.add(1, i + 200, i % 7)
    res = client.mrange(0, 500, filters=['Test=This'],
                     aggregation_type='avg', bucket_size_msec=10)
    assert 2 == len(res)
    assert 20 == len(res[0]['1'][1])
    
    # test withlabels
    assert {} == res[0]['1'][0]
    res = client.mrange(0, 200, filters=['Test=This'], with_labels=True)
    assert {'Test': 'This', 'team': 'ny'} == res[0]['1'][0]

    # available since RedisTimeSeries >= v1.4
    if version is None or version < 14000:
        return
    
    # test with selected labels
    res = client.mrange(0, 200, filters=['Test=This'], select_labels=['team'])
    assert {'team': 'ny'} == res[0]['1'][0]
    assert {'team': 'sf'} == res[1]['2'][0]
    # test with filterby
    res = client.mrange(0, 200, filters=['Test=This'], filter_by_ts=[i for i in range(10, 20)],
                        filter_by_min_value=1, filter_by_max_value=2)
    assert [(15, 1.0), (16, 2.0)] == res[0]['1'][1]
    # test groupby
    res = client.mrange(0, 3, filters=['Test=This'], groupby='Test', reduce='sum')
    assert [(0, 0.0), (1, 2.0), (2, 4.0), (3, 6.0)] == res[0]['Test=This'][1]
    res = client.mrange(0, 3, filters=['Test=This'], groupby='Test', reduce='max')
    assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[0]['Test=This'][1]
    res = client.mrange(0, 3, filters=['Test=This'], groupby='team', reduce='min')
    assert 2 == len(res)
    assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[0]['team=ny'][1]
    assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[1]['team=sf'][1]
    # test align
    res = client.mrange(0, 10, filters=['team=ny'], aggregation_type='count', bucket_size_msec=10, align='-')
    assert [(0, 10.0), (10, 1.0)] == res[0]['1'][1]
    res = client.mrange(0, 10, filters=['team=ny'], aggregation_type='count', bucket_size_msec=10, align=5)
    assert [(-5, 5.0), (5, 6.0)] == res[0]['1'][1]

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testMultiReverseRange(client):
    # TS.MREVRANGE is available since RedisTimeSeries >= v1.4
    if version is None or version < 14000:
        return

    client.create(1, labels={'Test': 'This', 'team': 'ny'})
    client.create(2, labels={'Test': 'This', 'Taste': 'That', 'team': 'sf'})
    for i in range(100):
        client.add(1, i, i % 7)
        client.add(2, i, i % 11)

    res = client.mrange(0, 200, filters=['Test=This'])
    assert 2 == len(res)
    assert 100 == len(res[0]['1'][1])

    res = client.mrange(0, 200, filters=['Test=This'], count=10)
    assert 10 == len(res[0]['1'][1])

    for i in range(100):
        client.add(1, i + 200, i % 7)
    res = client.mrevrange(0, 500, filters=['Test=This'],
                     aggregation_type='avg', bucket_size_msec=10)
    assert 2 == len(res)
    assert 20 == len(res[0]['1'][1])

    # test withlabels
    assert {} == res[0]['1'][0]
    res = client.mrevrange(0, 200, filters=['Test=This'], with_labels=True)
    assert {'Test': 'This', 'team': 'ny'} == res[0]['1'][0]
    # test with selected labels
    res = client.mrevrange(0, 200, filters=['Test=This'], select_labels=['team'])
    assert {'team': 'ny'} == res[0]['1'][0]
    assert {'team': 'sf'} == res[1]['2'][0]
    # test filterby
    res = client.mrevrange(0, 200, filters=['Test=This'], filter_by_ts=[i for i in range(10, 20)],
                        filter_by_min_value=1, filter_by_max_value=2)
    assert [(16, 2.0), (15, 1.0)] == res[0]['1'][1]
    # test groupby
    res = client.mrevrange(0, 3, filters=['Test=This'], groupby='Test', reduce='sum')
    assert [(3, 6.0), (2, 4.0), (1, 2.0), (0, 0.0)] == res[0]['Test=This'][1]
    res = client.mrevrange(0, 3, filters=['Test=This'], groupby='Test', reduce='max')
    assert [(3, 3.0), (2, 2.0),  (1, 1.0), (0, 0.0)] == res[0]['Test=This'][1]
    res = client.mrevrange(0, 3, filters=['Test=This'], groupby='team', reduce='min')
    assert 2 == len(res)
    assert [(3, 3.0), (2, 2.0),  (1, 1.0), (0, 0.0)] == res[0]['team=ny'][1]
    assert [(3, 3.0), (2, 2.0),  (1, 1.0), (0, 0.0)] == res[1]['team=sf'][1]
    # test align
    res = client.mrevrange(0, 10, filters=['team=ny'], aggregation_type='count', bucket_size_msec=10, align='-')
    assert [(10, 1.0), (0, 10.0)] == res[0]['1'][1]
    res = client.mrevrange(0, 10, filters=['team=ny'], aggregation_type='count', bucket_size_msec=10, align=1)
    assert [(1, 10.0), (-9, 1.0)] == res[0]['1'][1]

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testGet(client):
    name = 'test'
    client.create(name)
    assert client.get(name) is None
    client.add(name, 2, 3)
    assert 2 == client.get(name)[0]
    client.add(name, 3, 4)
    assert 4 == client.get(name)[1]

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testMGet(client):
    client.create(1, labels={'Test': 'This'})
    client.create(2, labels={'Test': 'This', 'Taste': 'That'})
    act_res = client.mget(['Test=This'])
    exp_res = [{'1': [{}, None, None]}, {'2': [{}, None, None]}]
    assert act_res == exp_res
    client.add(1, '*', 15)
    client.add(2, '*', 25)
    res = client.mget(['Test=This'])
    assert 15 == res[0]['1'][2]
    assert 25 == res[1]['2'][2]
    res = client.mget(['Taste=That'])
    assert 25 == res[0]['2'][2]

    # test with_labels
    assert {} == res[0]['2'][0]
    res = client.mget(['Taste=That'], with_labels=True)
    assert {'Taste': 'That', 'Test': 'This'} == res[0]['2'][0]

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testInfo(client):
    client.create(1, retention_msecs=5, labels={'currentLabel': 'currentData'})
    info = client.info(1)
    assert 5 == info.retention_msecs
    assert info.labels['currentLabel'] == 'currentData'
    if version is None or version < 14000:
        return
    assert info.duplicate_policy is None

    client.create('time-serie-2', duplicate_policy='min')
    info = client.info('time-serie-2')
    assert 'min' == info.duplicate_policy

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testQueryIndex(client):
    client.create(1, labels={'Test': 'This'})
    client.create(2, labels={'Test': 'This', 'Taste': 'That'})
    assert 2 == len(client.queryindex(['Test=This']))
    assert 1 == len(client.queryindex(['Taste=That']))
    assert ['2'] == client.queryindex(['Taste=That'])

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testPipeline(client):
    pipeline = client.pipeline()
    pipeline.create('with_pipeline')
    for i in range(100):
        pipeline.add('with_pipeline', i, 1.1 * i)
    pipeline.execute()

    info = client.info('with_pipeline')
    assert info.lastTimeStamp == 99
    assert info.total_samples == 100
    assert client.get('with_pipeline')[1] == 99 * 1.1

@pytest.mark.integrations
@pytest.mark.redistimeseries
def testUncompressed(client):
    client.create('compressed')
    client.create('uncompressed', uncompressed=True)
    compressed_info = client.info('compressed')
    uncompressed_info = client.info('uncompressed')
    assert compressed_info.memory_usage != uncompressed_info.memory_usage
