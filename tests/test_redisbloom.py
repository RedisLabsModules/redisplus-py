import pytest
from redis import Redis
from redisplus.client import RedisPlus

i = lambda l: [int(v) for v in l]


@pytest.fixture
def client():
    rc = RedisPlus(modules={"redisbloom": {"client": Redis()}})
    rc.redisbloom.flushdb()

    return rc.redisbloom


@pytest.mark.redisbloom
def test_base(client):
    rc = RedisPlus(modules={"redisbloom": {"client": Redis()}})
    rc.redisbloom.flushdb()


@pytest.mark.integrations
@pytest.mark.redisbloom
def testCreate(client):
    """Test CREATE/RESERVE calls"""
    assert client.bfCreate("bloom", 0.01, 1000)
    assert client.bfCreate("bloom_e", 0.01, 1000, expansion=1)
    assert client.bfCreate("bloom_ns", 0.01, 1000, noScale=True)
    assert client.cfCreate("cuckoo", 1000)
    assert client.cfCreate("cuckoo_e", 1000, expansion=1)
    assert client.cfCreate("cuckoo_bs", 1000, bucket_size=4)
    assert client.cfCreate("cuckoo_mi", 1000, max_iterations=10)
    assert client.cmsInitByDim("cmsDim", 100, 5)
    assert client.cmsInitByProb("cmsProb", 0.01, 0.01)
    assert client.topkReserve("topk", 5, 100, 5, 0.9)
    assert client.tdigestCreate("tDigest", 100)


# region Test Bloom Filter
@pytest.mark.integrations
@pytest.mark.redisbloom
def testBFAdd(client):
    assert client.bfCreate("bloom", 0.01, 1000)
    assert 1 == client.bfAdd("bloom", "foo")
    assert 0 == client.bfAdd("bloom", "foo")
    assert [0] == i(client.bfMAdd("bloom", "foo"))
    assert [0, 1] == client.bfMAdd("bloom", "foo", "bar")
    assert [0, 0, 1] == client.bfMAdd("bloom", "foo", "bar", "baz")
    assert 1 == client.bfExists("bloom", "foo")
    assert 0 == client.bfExists("bloom", "noexist")
    assert [1, 0] == i(client.bfMExists("bloom", "foo", "noexist"))


@pytest.mark.integrations
@pytest.mark.redisbloom
def testBFInsert(client):
    assert client.bfCreate("bloom", 0.01, 1000)
    assert [1] == i(client.bfInsert("bloom", ["foo"]))
    assert [0, 1] == i(client.bfInsert("bloom", ["foo", "bar"]))
    assert [1] == i(client.bfInsert("captest", ["foo"], capacity=1000))
    assert [1] == i(client.bfInsert("errtest", ["foo"], error=0.01))
    assert 1 == client.bfExists("bloom", "foo")
    assert 0 == client.bfExists("bloom", "noexist")
    assert [1, 0] == i(client.bfMExists("bloom", "foo", "noexist"))
    info = client.bfInfo("bloom")
    assert 2 == info.insertedNum
    assert 1000 == info.capacity
    assert 1 == info.filterNum


@pytest.mark.integrations
@pytest.mark.redisbloom
def testBFDumpLoad(client):
    # Store a filter
    client.bfCreate("myBloom", "0.0001", "1000")

    # test is probabilistic and might fail. It is OK to change variables if
    # certain to not break anything
    def do_verify():
        res = 0
        for x in range(1000):
            client.bfAdd("myBloom", x)
            rv = client.bfExists("myBloom", x)
            assert rv
            rv = client.bfExists("myBloom", "nonexist_{}".format(x))
            res += rv == x
        assert res < 5

    do_verify()
    cmds = []
    cur = client.bfScandump("myBloom", 0)
    first = cur[0]
    cmds.append(cur)

    while True:
        cur = client.bfScandump("myBloom", first)
        first = cur[0]
        if first == 0:
            break
        else:
            cmds.append(cur)
    prev_info = client.execute_command("bf.debug", "myBloom")

    # Remove the filter
    client.execute_command("del", "myBloom")

    # Now, load all the commands:
    for cmd in cmds:
        client.bfLoadChunk("myBloom", *cmd)

    cur_info = client.execute_command("bf.debug", "myBloom")
    assert prev_info == cur_info
    do_verify()

    client.execute_command("del", "myBloom")
    client.bfCreate("myBloom", "0.0001", "10000000")


@pytest.mark.integrations
@pytest.mark.redisbloom
def testBFInfo(client):
    expansion = 4
    # Store a filter
    client.bfCreate("nonscaling", "0.0001", "1000", noScale=True)
    info = client.bfInfo("nonscaling")
    assert info.expansionRate is None

    client.bfCreate("expanding", "0.0001", "1000", expansion=expansion)
    info = client.bfInfo("expanding")
    assert info.expansionRate == 4

    try:
        # noScale mean no expansion
        client.bfCreate("myBloom", "0.0001", "1000", expansion=expansion, noScale=True)
        assert False
    except:
        assert True


# endregion


# region Test Cuckoo Filter
@pytest.mark.integrations
@pytest.mark.redisbloom
def testCFAddInsert(client):
    assert client.cfCreate("cuckoo", 1000)
    assert client.cfAdd("cuckoo", "filter")
    assert not client.cfAddNX("cuckoo", "filter")
    assert 1 == client.cfAddNX("cuckoo", "newItem")
    assert [1] == client.cfInsert("captest", ["foo"])
    assert [1] == client.cfInsert("captest", ["foo"], capacity=1000)
    assert [1] == client.cfInsertNX("captest", ["bar"])
    assert [1] == client.cfInsertNX("captest", ["food"], nocreate="1")
    assert [0, 0, 1] == client.cfInsertNX("captest", ["foo", "bar", "baz"])
    assert [0] == client.cfInsertNX("captest", ["bar"], capacity=1000)
    assert [1] == client.cfInsert("empty1", ["foo"], capacity=1000)
    assert [1] == client.cfInsertNX("empty2", ["bar"], capacity=1000)
    info = client.cfInfo("captest")
    assert 5 == info.insertedNum
    assert 0 == info.deletedNum
    assert 1 == info.filterNum


@pytest.mark.integrations
@pytest.mark.redisbloom
def testCFExistsDel(client):
    assert client.cfCreate("cuckoo", 1000)
    assert client.cfAdd("cuckoo", "filter")
    assert client.cfExists("cuckoo", "filter")
    assert not client.cfExists("cuckoo", "notexist")
    assert 1 == client.cfCount("cuckoo", "filter")
    assert 0 == client.cfCount("cuckoo", "notexist")
    assert client.cfDel("cuckoo", "filter")
    assert 0 == client.cfCount("cuckoo", "filter")


# endregion


# region Test Count-Min Sketch
@pytest.mark.integrations
@pytest.mark.redisbloom
def testCMS(client):
    assert client.cmsInitByDim("dim", 1000, 5)
    assert client.cmsInitByProb("prob", 0.01, 0.01)
    assert client.cmsIncrBy("dim", ["foo"], [5])
    assert [0] == client.cmsQuery("dim", "notexist")
    assert [5] == client.cmsQuery("dim", "foo")
    assert [10, 15] == client.cmsIncrBy("dim", ["foo", "bar"], [5, 15])
    assert [10, 15] == client.cmsQuery("dim", "foo", "bar")
    info = client.cmsInfo("dim")
    assert 1000 == info.width
    assert 5 == info.depth
    assert 25 == info.count


@pytest.mark.integrations
@pytest.mark.redisbloom
def testCMSMerge(client):
    assert client.cmsInitByDim("A", 1000, 5)
    assert client.cmsInitByDim("B", 1000, 5)
    assert client.cmsInitByDim("C", 1000, 5)
    assert client.cmsIncrBy("A", ["foo", "bar", "baz"], [5, 3, 9])
    assert client.cmsIncrBy("B", ["foo", "bar", "baz"], [2, 3, 1])
    assert [5, 3, 9] == client.cmsQuery("A", "foo", "bar", "baz")
    assert [2, 3, 1] == client.cmsQuery("B", "foo", "bar", "baz")
    assert client.cmsMerge("C", 2, ["A", "B"])
    assert [7, 6, 10] == client.cmsQuery("C", "foo", "bar", "baz")
    assert client.cmsMerge("C", 2, ["A", "B"], ["1", "2"])
    assert [9, 9, 11] == client.cmsQuery("C", "foo", "bar", "baz")
    assert client.cmsMerge("C", 2, ["A", "B"], ["2", "3"])
    assert [16, 15, 21] == client.cmsQuery("C", "foo", "bar", "baz")


# endregion


# region Test Top-K
@pytest.mark.integrations
@pytest.mark.redisbloom
def testTopK(client):
    # test list with empty buckets
    assert client.topkReserve("topk", 3, 50, 4, 0.9)
    assert [
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "C",
        None,
        None,
        None,
        None,
    ] == client.topkAdd(
        "topk",
        "A",
        "B",
        "C",
        "D",
        "E",
        "A",
        "A",
        "B",
        "C",
        "G",
        "D",
        "B",
        "D",
        "A",
        "E",
        "E",
        1,
    )
    assert [1, 1, 0, 1, 0, 0, 0] == client.topkQuery(
        "topk", "A", "B", "C", "D", "E", "F", "G"
    )
    assert [4, 3, 2, 3, 3, 0, 1] == client.topkCount(
        "topk", "A", "B", "C", "D", "E", "F", "G"
    )

    # test full list
    assert client.topkReserve("topklist", 3, 50, 3, 0.9)
    assert client.topkAdd(
        "topklist",
        "A",
        "B",
        "C",
        "D",
        "E",
        "A",
        "A",
        "B",
        "C",
        "G",
        "D",
        "B",
        "D",
        "A",
        "E",
        "E",
    )
    assert ["D", "A", "B"] == client.topkList("topklist")
    info = client.topkInfo("topklist")
    assert 3 == info.k
    assert 50 == info.width
    assert 3 == info.depth
    assert 0.9 == round(float(info.decay), 1)


# endregion


# region Test T-Digest
@pytest.mark.integrations
@pytest.mark.redisbloom
def testTDigestReset(client):
    assert client.tdigestCreate("tDigest", 10)
    # reset on empty histogram
    assert client.tdigestReset("tDigest")
    # insert data-points into sketch
    assert client.tdigestAdd("tDigest", list(range(10)), [1.0] * 10)

    assert client.tdigestReset("tDigest")
    # assert we have 0 unmerged nodes
    assert 0 == client.tdigestInfo("tDigest").unmergedNodes


@pytest.mark.integrations
@pytest.mark.redisbloom
def testTDigestMerge(client):
    assert client.tdigestCreate("to-tDigest", 10)
    assert client.tdigestCreate("from-tDigest", 10)
    # insert data-points into sketch
    assert client.tdigestAdd("from-tDigest", [1.0] * 10, [1.0] * 10)
    assert client.tdigestAdd("to-tDigest", [2.0] * 10, [10.0] * 10)
    # merge from-tdigest into to-tdigest
    assert client.tdigestMerge("to-tDigest", "from-tDigest")
    # we should now have 110 weight on to-histogram
    info = client.tdigestInfo("to-tDigest")
    total_weight_to = float(info.mergedWeight) + float(info.unmergedWeight)
    assert 110 == total_weight_to


@pytest.mark.integrations
@pytest.mark.redisbloom
def testTDigestMinMax(client):
    assert client.tdigestCreate("tDigest", 100)
    # insert data-points into sketch
    assert client.tdigestAdd("tDigest", [1, 2, 3], [1.0] * 3)
    # min/max
    assert 3 == client.tdigestMax("tDigest")
    assert 1 == client.tdigestMin("tDigest")


@pytest.mark.integrations
@pytest.mark.redisbloom
def testTDigestQuantile(client):
    assert client.tdigestCreate("tDigest", 500)
    # insert data-points into sketch
    assert client.tdigestAdd(
        "tDigest", list([x * 0.01 for x in range(1, 10000)]), [1.0] * 10000
    )
    # assert min min/max have same result as quantile 0 and 1
    assert client.tdigestMax("tDigest") == client.tdigestQuantile("tDigest", 1.0)
    assert client.tdigestMin("tDigest") == client.tdigestQuantile("tDigest", 0.0)

    assert 1.0 == round(client.tdigestQuantile("tDigest", 0.01), 2)
    assert 99.0 == round(client.tdigestQuantile("tDigest", 0.99), 2)


@pytest.mark.integrations
@pytest.mark.redisbloom
def testTDigestCdf(client):
    assert client.tdigestCreate("tDigest", 100)
    # insert data-points into sketch
    assert client.tdigestAdd("tDigest", list(range(1, 10)), [1.0] * 10)

    assert 0.1 == round(client.tdigestCdf("tDigest", 1.0), 1)
    assert 0.9 == round(client.tdigestCdf("tDigest", 9.0), 1)


# endregion


"""
@pytest.mark.integrations
@pytest.mark.redisbloom
def test_pipeline(client):
    pipeline = client.pipeline()

    assert not client.execute_command('get pipeline')

    assert (client.bfCreate('pipeline', 0.01, 1000))
    for i in range(100):
        pipeline.bfAdd('pipeline', i)
    for i in range(100):
        assert not (client.bfExists('pipeline', i))

    pipeline.execute()

    for i in range(100):
        assert (client.bfExists('pipeline', i))
"""
