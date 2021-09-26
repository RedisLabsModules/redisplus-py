import pytest
import time
from time import sleep
from redisplus.client import Client
from .conftest import skip_ifmodversion_lt


@pytest.fixture
def client():
    rc = Client()
    rc.flushdb()
    return rc


@pytest.mark.integrations
@pytest.mark.timeseries
def testCreate(client):
    assert client.tf.create(1)
    assert client.tf.create(2, retention_msecs=5)
    assert client.tf.create(3, labels={"Redis": "Labs"})
    assert client.tf.create(4, retention_msecs=20, labels={"Time": "Series"})
    info = client.tf.info(4)
    assert 20 == info.retention_msecs
    assert "Series" == info.labels["Time"]

    # Test for a chunk size of 128 Bytes
    assert client.tf.create("time-serie-1", chunk_size=128)
    info = client.tf.info("time-serie-1")
    assert 128, info.chunk_size


@pytest.mark.integrations
@pytest.mark.timeseries
@skip_ifmodversion_lt("1.4.0", "timeseries")
def testCreateDuplicatePolicy(client):
    # Test for duplicate policy
    for duplicate_policy in ["block", "last", "first", "min", "max"]:
        ts_name = "time-serie-ooo-{0}".format(duplicate_policy)
        assert client.tf.create(ts_name, duplicate_policy=duplicate_policy)
        info = client.tf.info(ts_name)
        assert duplicate_policy == info.duplicate_policy


@pytest.mark.integrations
@pytest.mark.timeseries
def testAlter(client):
    assert client.tf.create(1)
    assert 0 == client.tf.info(1).retention_msecs
    assert client.tf.alter(1, retention_msecs=10)
    assert {} == client.tf.info(1).labels
    assert 10, client.tf.info(1).retention_msecs
    assert client.tf.alter(1, labels={"Time": "Series"})
    assert "Series" == client.tf.info(1).labels["Time"]
    assert 10 == client.tf.info(1).retention_msecs
    pipe = client.tf.pipeline()
    assert pipe.create(2)


@pytest.mark.integrations
@pytest.mark.timeseries
@skip_ifmodversion_lt("1.4.0", "timeseries")
def testAlterDiplicatePolicy(client):
    assert client.tf.create(1)
    info = client.tf.info(1)
    assert info.duplicate_policy is None
    assert client.tf.alter(1, duplicate_policy="min")
    info = client.tf.info(1)
    assert "min" == info.duplicate_policy


@pytest.mark.integrations
@pytest.mark.timeseries
def testAdd(client):
    assert 1 == client.tf.add(1, 1, 1)
    assert 2 == client.tf.add(2, 2, 3, retention_msecs=10)
    assert 3 == client.tf.add(3, 3, 2, labels={"Redis": "Labs"})
    assert 4 == client.tf.add(
        4, 4, 2, retention_msecs=10, labels={"Redis": "Labs", "Time": "Series"}
    )
    assert round(time.time()) == round(float(client.tf.add(5, "*", 1)) / 1000)

    info = client.tf.info(4)
    assert 10 == info.retention_msecs
    assert "Labs" == info.labels["Redis"]

    # Test for a chunk size of 128 Bytes on TS.ADD
    assert client.tf.add("time-serie-1", 1, 10.0, chunk_size=128)
    info = client.tf.info("time-serie-1")
    assert 128 == info.chunk_size


@pytest.mark.integrations
@pytest.mark.timeseries
@skip_ifmodversion_lt("1.4.0", "timeseries")
def testAddDuplicatePolicy(client):

    # Test for duplicate policy BLOCK
    assert 1 == client.tf.add("time-serie-add-ooo-block", 1, 5.0)
    with pytest.raises(Exception):
        client.tf.add("time-serie-add-ooo-block", 1, 5.0, duplicate_policy="block")

    # Test for duplicate policy LAST
    assert 1 == client.tf.add("time-serie-add-ooo-last", 1, 5.0)
    assert 1 == client.tf.add(
        "time-serie-add-ooo-last", 1, 10.0, duplicate_policy="last"
    )
    assert 10.0 == client.tf.get("time-serie-add-ooo-last")[1]

    # Test for duplicate policy FIRST
    assert 1 == client.tf.add("time-serie-add-ooo-first", 1, 5.0)
    assert 1 == client.tf.add(
        "time-serie-add-ooo-first", 1, 10.0, duplicate_policy="first"
    )
    assert 5.0 == client.tf.get("time-serie-add-ooo-first")[1]

    # Test for duplicate policy MAX
    assert 1 == client.tf.add("time-serie-add-ooo-max", 1, 5.0)
    assert 1 == client.tf.add("time-serie-add-ooo-max", 1, 10.0, duplicate_policy="max")
    assert 10.0 == client.tf.get("time-serie-add-ooo-max")[1]

    # Test for duplicate policy MIN
    assert 1 == client.tf.add("time-serie-add-ooo-min", 1, 5.0)
    assert 1 == client.tf.add("time-serie-add-ooo-min", 1, 10.0, duplicate_policy="min")
    assert 5.0 == client.tf.get("time-serie-add-ooo-min")[1]


@pytest.mark.integrations
@pytest.mark.timeseries
def testMAdd(client):
    client.tf.create("a")
    assert [1, 2, 3] == client.tf.madd([("a", 1, 5), ("a", 2, 10), ("a", 3, 15)])


@pytest.mark.integrations
@pytest.mark.timeseries
def testIncrbyDecrby(client):
    for _ in range(100):
        assert client.tf.incrby(1, 1)
        sleep(0.001)
    assert 100 == client.tf.get(1)[1]
    for _ in range(100):
        assert client.tf.decrby(1, 1)
        sleep(0.001)
    assert 0 == client.tf.get(1)[1]

    assert client.tf.incrby(2, 1.5, timestamp=5)
    assert (5, 1.5) == client.tf.get(2)
    assert client.tf.incrby(2, 2.25, timestamp=7)
    assert (7, 3.75) == client.tf.get(2)
    assert client.tf.decrby(2, 1.5, timestamp=15)
    assert (15, 2.25) == client.tf.get(2)

    # Test for a chunk size of 128 Bytes on TS.INCRBY
    assert client.tf.incrby("time-serie-1", 10, chunk_size=128)
    info = client.tf.info("time-serie-1")
    assert 128 == info.chunk_size

    # Test for a chunk size of 128 Bytes on TS.DECRBY
    assert client.tf.decrby("time-serie-2", 10, chunk_size=128)
    info = client.tf.info("time-serie-2")
    assert 128 == info.chunk_size


@pytest.mark.integrations
@pytest.mark.timeseries
def testCreateAndDeleteRule(client):
    # test rule creation
    time = 100
    client.tf.create(1)
    client.tf.create(2)
    client.tf.createrule(1, 2, "avg", 100)
    for i in range(50):
        client.tf.add(1, time + i * 2, 1)
        client.tf.add(1, time + i * 2 + 1, 2)
    client.tf.add(1, time * 2, 1.5)
    assert round(client.tf.get(2)[1], 5) == 1.5
    info = client.tf.info(1)
    assert info.rules[0][1] == 100

    # test rule deletion
    client.tf.deleterule(1, 2)
    info = client.tf.info(1)
    assert not info.rules


@pytest.mark.integrations
@pytest.mark.timeseries
@skip_ifmodversion_lt("99.99.99", "timeseries")  # todo: update after the release
def testDelRange(client):
    try:
        client.tf.delete("test", 0, 100)
    except Exception as e:
        assert e.__str__() != ""

    for i in range(100):
        client.tf.add(1, i, i % 7)
    assert 22 == client.tf.delete(1, 0, 21)
    assert [] == client.tf.range(1, 0, 21)
    assert [(22, 1.0)] == client.tf.range(1, 22, 22)


@pytest.mark.integrations
@pytest.mark.timeseries
def testRange(client):
    for i in range(100):
        client.tf.add(1, i, i % 7)
    assert 100 == len(client.tf.range(1, 0, 200))
    for i in range(100):
        client.tf.add(1, i + 200, i % 7)
    assert 200 == len(client.tf.range(1, 0, 500))
    # last sample isn't returned
    assert 20 == len(
        client.tf.range(1, 0, 500, aggregation_type="avg", bucket_size_msec=10)
    )
    assert 10 == len(client.tf.range(1, 0, 500, count=10))


@pytest.mark.integrations
@pytest.mark.timeseries
@skip_ifmodversion_lt("99.99.99", "timeseries")  # todo: update after the release
def testRangeAdvanced(client):
    for i in range(100):
        client.tf.add(1, i, i % 7)
        client.tf.add(1, i + 200, i % 7)

    assert 2 == len(
        client.tf.range(
            1,
            0,
            500,
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
    )
    assert [(0, 10.0), (10, 1.0)] == client.tf.range(
        1, 0, 10, aggregation_type="count", bucket_size_msec=10, align="+"
    )
    assert [(-5, 5.0), (5, 6.0)] == client.tf.range(
        1, 0, 10, aggregation_type="count", bucket_size_msec=10, align=5
    )


@pytest.mark.integrations
@pytest.mark.timeseries
@skip_ifmodversion_lt("99.99.99", "timeseries")  # todo: update after the release
def testRevRange(client):
    for i in range(100):
        client.tf.add(1, i, i % 7)
    assert 100 == len(client.tf.range(1, 0, 200))
    for i in range(100):
        client.tf.add(1, i + 200, i % 7)
    assert 200 == len(client.tf.range(1, 0, 500))
    # first sample isn't returned
    assert 20 == len(
        client.tf.revrange(1, 0, 500, aggregation_type="avg", bucket_size_msec=10)
    )
    assert 10 == len(client.tf.revrange(1, 0, 500, count=10))
    assert 2 == len(
        client.tf.revrange(
            1,
            0,
            500,
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
    )
    assert [(10, 1.0), (0, 10.0)] == client.tf.revrange(
        1, 0, 10, aggregation_type="count", bucket_size_msec=10, align="+"
    )
    assert [(1, 10.0), (-9, 1.0)] == client.tf.revrange(
        1, 0, 10, aggregation_type="count", bucket_size_msec=10, align=1
    )


@pytest.mark.integrations
@pytest.mark.timeseries
def testMultiRange(client):
    client.tf.create(1, labels={"Test": "This", "team": "ny"})
    client.tf.create(2, labels={"Test": "This", "Taste": "That", "team": "sf"})
    for i in range(100):
        client.tf.add(1, i, i % 7)
        client.tf.add(2, i, i % 11)

    res = client.tf.mrange(0, 200, filters=["Test=This"])
    assert 2 == len(res)
    assert 100 == len(res[0]["1"][1])

    res = client.tf.mrange(0, 200, filters=["Test=This"], count=10)
    assert 10 == len(res[0]["1"][1])

    for i in range(100):
        client.tf.add(1, i + 200, i % 7)
    res = client.tf.mrange(
        0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
    )
    assert 2 == len(res)
    assert 20 == len(res[0]["1"][1])

    # test withlabels
    assert {} == res[0]["1"][0]
    res = client.tf.mrange(0, 200, filters=["Test=This"], with_labels=True)
    assert {"Test": "This", "team": "ny"} == res[0]["1"][0]


@pytest.mark.integrations
@pytest.mark.timeseries
@skip_ifmodversion_lt("99.99.99", "timeseries")  # todo: update after the release
def testMultiRangeAdvanced(client):
    client.tf.create(1, labels={"Test": "This", "team": "ny"})
    client.tf.create(2, labels={"Test": "This", "Taste": "That", "team": "sf"})
    for i in range(100):
        client.tf.add(1, i, i % 7)
        client.tf.add(2, i, i % 11)

    # test with selected labels
    res = client.tf.mrange(0, 200, filters=["Test=This"], select_labels=["team"])
    assert {"team": "ny"} == res[0]["1"][0]
    assert {"team": "sf"} == res[1]["2"][0]

    # test with filterby
    res = client.tf.mrange(
        0,
        200,
        filters=["Test=This"],
        filter_by_ts=[i for i in range(10, 20)],
        filter_by_min_value=1,
        filter_by_max_value=2,
    )
    assert [(15, 1.0), (16, 2.0)] == res[0]["1"][1]

    # test groupby
    res = client.tf.mrange(0, 3, filters=["Test=This"], groupby="Test", reduce="sum")
    assert [(0, 0.0), (1, 2.0), (2, 4.0), (3, 6.0)] == res[0]["Test=This"][1]
    res = client.tf.mrange(0, 3, filters=["Test=This"], groupby="Test", reduce="max")
    assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[0]["Test=This"][1]
    res = client.tf.mrange(0, 3, filters=["Test=This"], groupby="team", reduce="min")
    assert 2 == len(res)
    assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[0]["team=ny"][1]
    assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[1]["team=sf"][1]

    # test align
    res = client.tf.mrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align="-",
    )
    assert [(0, 10.0), (10, 1.0)] == res[0]["1"][1]
    res = client.tf.mrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align=5,
    )
    assert [(-5, 5.0), (5, 6.0)] == res[0]["1"][1]


@pytest.mark.integrations
@pytest.mark.timeseries
@skip_ifmodversion_lt("99.99.99", "timeseries")  # todo: update after the release
def testMultiReverseRange(client):
    client.tf.create(1, labels={"Test": "This", "team": "ny"})
    client.tf.create(2, labels={"Test": "This", "Taste": "That", "team": "sf"})
    for i in range(100):
        client.tf.add(1, i, i % 7)
        client.tf.add(2, i, i % 11)

    res = client.tf.mrange(0, 200, filters=["Test=This"])
    assert 2 == len(res)
    assert 100 == len(res[0]["1"][1])

    res = client.tf.mrange(0, 200, filters=["Test=This"], count=10)
    assert 10 == len(res[0]["1"][1])

    for i in range(100):
        client.tf.add(1, i + 200, i % 7)
    res = client.tf.mrevrange(
        0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
    )
    assert 2 == len(res)
    assert 20 == len(res[0]["1"][1])
    assert {} == res[0]["1"][0]

    # test withlabels
    res = client.tf.mrevrange(0, 200, filters=["Test=This"], with_labels=True)
    assert {"Test": "This", "team": "ny"} == res[0]["1"][0]

    # test with selected labels
    res = client.tf.mrevrange(0, 200, filters=["Test=This"], select_labels=["team"])
    assert {"team": "ny"} == res[0]["1"][0]
    assert {"team": "sf"} == res[1]["2"][0]

    # test filterby
    res = client.tf.mrevrange(
        0,
        200,
        filters=["Test=This"],
        filter_by_ts=[i for i in range(10, 20)],
        filter_by_min_value=1,
        filter_by_max_value=2,
    )
    assert [(16, 2.0), (15, 1.0)] == res[0]["1"][1]

    # test groupby
    res = client.tf.mrevrange(0, 3, filters=["Test=This"], groupby="Test", reduce="sum")
    assert [(3, 6.0), (2, 4.0), (1, 2.0), (0, 0.0)] == res[0]["Test=This"][1]
    res = client.tf.mrevrange(0, 3, filters=["Test=This"], groupby="Test", reduce="max")
    assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[0]["Test=This"][1]
    res = client.tf.mrevrange(0, 3, filters=["Test=This"], groupby="team", reduce="min")
    assert 2 == len(res)
    assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[0]["team=ny"][1]
    assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[1]["team=sf"][1]

    # test align
    res = client.tf.mrevrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align="-",
    )
    assert [(10, 1.0), (0, 10.0)] == res[0]["1"][1]
    res = client.tf.mrevrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align=1,
    )
    assert [(1, 10.0), (-9, 1.0)] == res[0]["1"][1]


@pytest.mark.integrations
@pytest.mark.timeseries
def testGet(client):
    name = "test"
    client.tf.create(name)
    assert client.tf.get(name) is None
    client.tf.add(name, 2, 3)
    assert 2 == client.tf.get(name)[0]
    client.tf.add(name, 3, 4)
    assert 4 == client.tf.get(name)[1]


@pytest.mark.integrations
@pytest.mark.timeseries
def testMGet(client):
    client.tf.create(1, labels={"Test": "This"})
    client.tf.create(2, labels={"Test": "This", "Taste": "That"})
    act_res = client.tf.mget(["Test=This"])
    exp_res = [{"1": [{}, None, None]}, {"2": [{}, None, None]}]
    assert act_res == exp_res
    client.tf.add(1, "*", 15)
    client.tf.add(2, "*", 25)
    res = client.tf.mget(["Test=This"])
    assert 15 == res[0]["1"][2]
    assert 25 == res[1]["2"][2]
    res = client.tf.mget(["Taste=That"])
    assert 25 == res[0]["2"][2]

    # test with_labels
    assert {} == res[0]["2"][0]
    res = client.tf.mget(["Taste=That"], with_labels=True)
    assert {"Taste": "That", "Test": "This"} == res[0]["2"][0]


@pytest.mark.integrations
@pytest.mark.timeseries
def testInfo(client):
    client.tf.create(1, retention_msecs=5, labels={"currentLabel": "currentData"})
    info = client.tf.info(1)
    assert 5 == info.retention_msecs
    assert info.labels["currentLabel"] == "currentData"


@pytest.mark.integrations
@pytest.mark.timeseries
@skip_ifmodversion_lt("1.4.0", "timeseries")
def testInfoDuplicatePolicy(client):
    client.tf.create(1, retention_msecs=5, labels={"currentLabel": "currentData"})
    info = client.tf.info(1)
    assert info.duplicate_policy is None

    client.tf.create("time-serie-2", duplicate_policy="min")
    info = client.tf.info("time-serie-2")
    assert "min" == info.duplicate_policy


@pytest.mark.integrations
@pytest.mark.timeseries
def testQueryIndex(client):
    client.tf.create(1, labels={"Test": "This"})
    client.tf.create(2, labels={"Test": "This", "Taste": "That"})
    assert 2 == len(client.tf.queryindex(["Test=This"]))
    assert 1 == len(client.tf.queryindex(["Taste=That"]))
    assert [2] == client.tf.queryindex(["Taste=That"])


@pytest.mark.integrations
@pytest.mark.timeseries
@pytest.mark.pipeline
def testPipeline(client):
    pipeline = client.tf.pipeline()
    pipeline.create("with_pipeline")
    for i in range(100):
        pipeline.add("with_pipeline", i, 1.1 * i)
    pipeline.execute()

    info = client.tf.info("with_pipeline")
    assert info.lastTimeStamp == 99
    assert info.total_samples == 100
    assert client.tf.get("with_pipeline")[1] == 99 * 1.1


@pytest.mark.integrations
@pytest.mark.timeseries
def testUncompressed(client):
    client.tf.create("compressed")
    client.tf.create("uncompressed", uncompressed=True)
    compressed_info = client.tf.info("compressed")
    uncompressed_info = client.tf.info("uncompressed")
    assert compressed_info.memory_usage != uncompressed_info.memory_usage
