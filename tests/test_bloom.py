import pytest
from redis import Redis
import redisplus.bf
from redisplus.client import Client

i = lambda l: [int(v) for v in l]


@pytest.fixture
def client():
    rc = Client(Redis())  # modules={'bf': {"client": Redis()}})
    assert isinstance(rc.bf, redisplus.bf.Bloom)
    rc.flushdb()

    return rc


@pytest.mark.integrations
@pytest.mark.bloom
def testCreate(client):
    """Test CREATE/RESERVE calls"""
    assert client.bf.bfcreate("bloom", 0.01, 1000)
    assert client.bf.bfcreate("bloom_e", 0.01, 1000, expansion=1)
    assert client.bf.bfcreate("bloom_ns", 0.01, 1000, noScale=True)
    assert client.bf.cfcreate("cuckoo", 1000)
    assert client.bf.cfcreate("cuckoo_e", 1000, expansion=1)
    assert client.bf.cfcreate("cuckoo_bs", 1000, bucket_size=4)
    assert client.bf.cfcreate("cuckoo_mi", 1000, max_iterations=10)
    assert client.bf.cmsinitbydim("cmsDim", 100, 5)
    assert client.bf.cmsinitbyprob("cmsProb", 0.01, 0.01)
    assert client.bf.topkreserve("topk", 5, 100, 5, 0.9)
    assert client.bf.tdigestcreate("tDigest", 100)


# region Test Bloom Filter
@pytest.mark.integrations
@pytest.mark.bloom
def testBFAdd(client):
    assert client.bf.bfcreate("bloom", 0.01, 1000)
    assert 1 == client.bf.bfadd("bloom", "foo")
    assert 0 == client.bf.bfadd("bloom", "foo")
    assert [0] == i(client.bf.bfmadd("bloom", "foo"))
    assert [0, 1] == client.bf.bfmadd("bloom", "foo", "bar")
    assert [0, 0, 1] == client.bf.bfmadd("bloom", "foo", "bar", "baz")
    assert 1 == client.bf.bfexists("bloom", "foo")
    assert 0 == client.bf.bfexists("bloom", "noexist")
    assert [1, 0] == i(client.bf.bfmexists("bloom", "foo", "noexist"))


@pytest.mark.integrations
@pytest.mark.bloom
def testBFInsert(client):
    assert client.bf.bfcreate("bloom", 0.01, 1000)
    assert [1] == i(client.bf.bfinsert("bloom", ["foo"]))
    assert [0, 1] == i(client.bf.bfinsert("bloom", ["foo", "bar"]))
    assert [1] == i(client.bf.bfinsert("captest", ["foo"], capacity=1000))
    assert [1] == i(client.bf.bfinsert("errtest", ["foo"], error=0.01))
    assert 1 == client.bf.bfexists("bloom", "foo")
    assert 0 == client.bf.bfexists("bloom", "noexist")
    assert [1, 0] == i(client.bf.bfmexists("bloom", "foo", "noexist"))
    info = client.bf.bfinfo("bloom")
    assert 2 == info.insertedNum
    assert 1000 == info.capacity
    assert 1 == info.filterNum


@pytest.mark.integrations
@pytest.mark.bloom
def testBFDumpLoad(client):
    # Store a filter
    client.bf.bfcreate("myBloom", "0.0001", "1000")

    # test is probabilistic and might fail. It is OK to change variables if
    # certain to not break anything
    def do_verify():
        res = 0
        for x in range(1000):
            client.bf.bfadd("myBloom", x)
            rv = client.bf.bfexists("myBloom", x)
            assert rv
            rv = client.bf.bfexists("myBloom", "nonexist_{}".format(x))
            res += rv == x
        assert res < 5

    do_verify()
    cmds = []
    cur = client.bf.bfscandump("myBloom", 0)
    first = cur[0]
    cmds.append(cur)

    while True:
        cur = client.bf.bfscandump("myBloom", first)
        first = cur[0]
        if first == 0:
            break
        else:
            cmds.append(cur)
    prev_info = client.bf.execute_command("bf.debug", "myBloom")

    # Remove the filter
    client.bf.execute_command("del", "myBloom")

    # Now, load all the commands:
    for cmd in cmds:
        client.bf.bfloadchunk("myBloom", *cmd)

    cur_info = client.bf.execute_command("bf.debug", "myBloom")
    assert prev_info == cur_info
    do_verify()

    client.bf.execute_command("del", "myBloom")
    client.bf.bfcreate("myBloom", "0.0001", "10000000")


@pytest.mark.integrations
@pytest.mark.bloom
def testBFInfo(client):
    expansion = 4
    # Store a filter
    client.bf.bfcreate("nonscaling", "0.0001", "1000", noScale=True)
    info = client.bf.bfinfo("nonscaling")
    assert info.expansionRate is None

    client.bf.bfcreate("expanding", "0.0001", "1000", expansion=expansion)
    info = client.bf.bfinfo("expanding")
    assert info.expansionRate == 4

    try:
        # noScale mean no expansion
        client.bf.bfcreate(
            "myBloom", "0.0001", "1000", expansion=expansion, noScale=True
        )
        assert False
    except:
        assert True


# endregion


# region Test Cuckoo Filter
@pytest.mark.integrations
@pytest.mark.bloom
def testCFAddInsert(client):
    assert client.bf.cfcreate("cuckoo", 1000)
    assert client.bf.cfadd("cuckoo", "filter")
    assert not client.bf.cfaddnx("cuckoo", "filter")
    assert 1 == client.bf.cfaddnx("cuckoo", "newItem")
    assert [1] == client.bf.cfinsert("captest", ["foo"])
    assert [1] == client.bf.cfinsert("captest", ["foo"], capacity=1000)
    assert [1] == client.bf.cfinsertnx("captest", ["bar"])
    assert [1] == client.bf.cfinsertnx("captest", ["food"], nocreate="1")
    assert [0, 0, 1] == client.bf.cfinsertnx("captest", ["foo", "bar", "baz"])
    assert [0] == client.bf.cfinsertnx("captest", ["bar"], capacity=1000)
    assert [1] == client.bf.cfinsert("empty1", ["foo"], capacity=1000)
    assert [1] == client.bf.cfinsertnx("empty2", ["bar"], capacity=1000)
    info = client.bf.cfinfo("captest")
    assert 5 == info.insertedNum
    assert 0 == info.deletedNum
    assert 1 == info.filterNum


@pytest.mark.integrations
@pytest.mark.bloom
def testCFExistsDel(client):
    assert client.bf.cfcreate("cuckoo", 1000)
    assert client.bf.cfadd("cuckoo", "filter")
    assert client.bf.cfexists("cuckoo", "filter")
    assert not client.bf.cfexists("cuckoo", "notexist")
    assert 1 == client.bf.cfcount("cuckoo", "filter")
    assert 0 == client.bf.cfcount("cuckoo", "notexist")
    assert client.bf.cfdel("cuckoo", "filter")
    assert 0 == client.bf.cfcount("cuckoo", "filter")


# endregion


# region Test Count-Min Sketch
@pytest.mark.integrations
@pytest.mark.bloom
def testCMS(client):
    assert client.bf.cmsinitbydim("dim", 1000, 5)
    assert client.bf.cmsinitbyprob("prob", 0.01, 0.01)
    assert client.bf.cmsincrby("dim", ["foo"], [5])
    assert [0] == client.bf.cmsquery("dim", "notexist")
    assert [5] == client.bf.cmsquery("dim", "foo")
    assert [10, 15] == client.bf.cmsincrby("dim", ["foo", "bar"], [5, 15])
    assert [10, 15] == client.bf.cmsquery("dim", "foo", "bar")
    info = client.bf.cmsinfo("dim")
    assert 1000 == info.width
    assert 5 == info.depth
    assert 25 == info.count


@pytest.mark.integrations
@pytest.mark.bloom
def testCMSMerge(client):
    assert client.bf.cmsinitbydim("A", 1000, 5)
    assert client.bf.cmsinitbydim("B", 1000, 5)
    assert client.bf.cmsinitbydim("C", 1000, 5)
    assert client.bf.cmsincrby("A", ["foo", "bar", "baz"], [5, 3, 9])
    assert client.bf.cmsincrby("B", ["foo", "bar", "baz"], [2, 3, 1])
    assert [5, 3, 9] == client.bf.cmsquery("A", "foo", "bar", "baz")
    assert [2, 3, 1] == client.bf.cmsquery("B", "foo", "bar", "baz")
    assert client.bf.cmsmerge("C", 2, ["A", "B"])
    assert [7, 6, 10] == client.bf.cmsquery("C", "foo", "bar", "baz")
    assert client.bf.cmsmerge("C", 2, ["A", "B"], ["1", "2"])
    assert [9, 9, 11] == client.bf.cmsquery("C", "foo", "bar", "baz")
    assert client.bf.cmsmerge("C", 2, ["A", "B"], ["2", "3"])
    assert [16, 15, 21] == client.bf.cmsquery("C", "foo", "bar", "baz")


# endregion


# region Test Top-K
@pytest.mark.integrations
@pytest.mark.bloom
def testTopK(client):
    # test list with empty buckets
    assert client.bf.topkreserve("topk", 3, 50, 4, 0.9)
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
    ] == client.bf.topkadd(
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
    assert [1, 1, 0, 1, 0, 0, 0] == client.bf.topkquery(
        "topk", "A", "B", "C", "D", "E", "F", "G"
    )
    assert [4, 3, 2, 3, 3, 0, 1] == client.bf.topkcount(
        "topk", "A", "B", "C", "D", "E", "F", "G"
    )

    # test full list
    assert client.bf.topkreserve("topklist", 3, 50, 3, 0.9)
    assert client.bf.topkadd(
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
    assert ["D", "A", "B"] == client.bf.topklist("topklist")
    info = client.bf.topkinfo("topklist")
    assert 3 == info.k
    assert 50 == info.width
    assert 3 == info.depth
    assert 0.9 == round(float(info.decay), 1)


# endregion


# region Test T-Digest
@pytest.mark.integrations
@pytest.mark.bloom
def testTDigestReset(client):
    assert client.bf.tdigestcreate("tDigest", 10)
    # reset on empty histogram
    assert client.bf.tdigestreset("tDigest")
    # insert data-points into sketch
    assert client.bf.tdigestadd("tDigest", list(range(10)), [1.0] * 10)

    assert client.bf.tdigestreset("tDigest")
    # assert we have 0 unmerged nodes
    assert 0 == client.bf.tdigestinfo("tDigest").unmergedNodes


@pytest.mark.integrations
@pytest.mark.bloom
def testTDigestMerge(client):
    assert client.bf.tdigestcreate("to-tDigest", 10)
    assert client.bf.tdigestcreate("from-tDigest", 10)
    # insert data-points into sketch
    assert client.bf.tdigestadd("from-tDigest", [1.0] * 10, [1.0] * 10)
    assert client.bf.tdigestadd("to-tDigest", [2.0] * 10, [10.0] * 10)
    # merge from-tdigest into to-tdigest
    assert client.bf.tdigestmerge("to-tDigest", "from-tDigest")
    # we should now have 110 weight on to-histogram
    info = client.bf.tdigestinfo("to-tDigest")
    total_weight_to = float(info.mergedWeight) + float(info.unmergedWeight)
    assert 110 == total_weight_to


@pytest.mark.integrations
@pytest.mark.bloom
def testTDigestMinMax(client):
    assert client.bf.tdigestcreate("tDigest", 100)
    # insert data-points into sketch
    assert client.bf.tdigestadd("tDigest", [1, 2, 3], [1.0] * 3)
    # min/max
    assert 3 == client.bf.tdigestmax("tDigest")
    assert 1 == client.bf.tdigestmin("tDigest")


@pytest.mark.integrations
@pytest.mark.bloom
def testTDigestQuantile(client):
    assert client.bf.tdigestcreate("tDigest", 500)
    # insert data-points into sketch
    assert client.bf.tdigestadd(
        "tDigest", list([x * 0.01 for x in range(1, 10000)]), [1.0] * 10000
    )
    # assert min min/max have same result as quantile 0 and 1
    assert client.bf.tdigestmax("tDigest") == client.bf.tdigestquantile("tDigest", 1.0)
    assert client.bf.tdigestmin("tDigest") == client.bf.tdigestquantile("tDigest", 0.0)

    assert 1.0 == round(client.bf.tdigestquantile("tDigest", 0.01), 2)
    assert 99.0 == round(client.bf.tdigestquantile("tDigest", 0.99), 2)


@pytest.mark.integrations
@pytest.mark.bloom
def testTDigestCdf(client):
    assert client.bf.tdigestcreate("tDigest", 100)
    # insert data-points into sketch
    assert client.bf.tdigestadd("tDigest", list(range(1, 10)), [1.0] * 10)

    assert 0.1 == round(client.bf.tdigestcdf("tDigest", 1.0), 1)
    assert 0.9 == round(client.bf.tdigestcdf("tDigest", 9.0), 1)


# endregion


@pytest.mark.integrations
@pytest.mark.bloom
def test_pipeline(client):
    pipeline = client.bf.pipeline()
    assert not client.bf.execute_command("get pipeline")

    assert client.bf.bfcreate("pipeline", 0.01, 1000)
    for i in range(100):
        pipeline.bfadd("pipeline", i)
    for i in range(100):
        assert not (client.bf.bfexists("pipeline", i))

    pipeline.execute()

    for i in range(100):
        assert client.bf.bfexists("pipeline", i)
