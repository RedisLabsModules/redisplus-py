import pytest
import redis
from redis import Redis
import redisplus.json
from redisplus import Client
from redisplus.json.path import Path
from .conftest import skip_ifmodversion_lt


@pytest.fixture
def client():
    rc = Client(Redis())
    assert isinstance(rc.json, redisplus.json.JSON)
    rc.flushdb()
    return rc.json


@pytest.mark.integrations
@pytest.mark.json
def test_json_setgetdeleteforget(client):
    assert client.jsonset("foo", Path.rootPath(), "bar")
    assert client.jsonget("foo") == "bar"
    assert client.jsonget("baz") is None
    assert client.jsondel("foo") == 1
    assert client.jsonforget("foo") == 0  # second delete
    assert client.client.exists("foo") == 0


@pytest.mark.integrations
@pytest.mark.json
def test_justaget(client):
    client.jsonset("foo", Path.rootPath(), "bar")
    assert client.jsonget("foo") == "bar"


@pytest.mark.integrations
@pytest.mark.json
def test_json_get_jset(client):
    assert client.jsonset("foo", Path.rootPath(), "bar")
    assert "bar" == client.jsonget("foo")
    assert None == client.jsonget("baz")
    assert 1 == client.jsondel("foo")
    assert client.client.exists("foo") == 0


@pytest.mark.integrations
@pytest.mark.json
def test_nonascii_setgetdelete(client):
    assert client.jsonset("notascii", Path.rootPath(), "hyvää-élève") is True
    assert "hyvää-élève" == client.jsonget("notascii", no_escape=True)
    assert 1 == client.jsondel("notascii")
    assert client.client.exists("notascii") == 0


@pytest.mark.integrations
@pytest.mark.json
def test_jsonsetexistentialmodifiersshouldsucceed(client):
    obj = {"foo": "bar"}
    assert client.jsonset("obj", Path.rootPath(), obj)

    # Test that flags prevent updates when conditions are unmet
    assert client.jsonset("obj", Path("foo"), "baz", nx=True) is None
    assert client.jsonset("obj", Path("qaz"), "baz", xx=True) is None

    # Test that flags allow updates when conditions are met
    assert client.jsonset("obj", Path("foo"), "baz", xx=True)
    assert client.jsonset("obj", Path("qaz"), "baz", nx=True)

    # Test that flags are mutually exlusive
    with pytest.raises(Exception):
        client.jsonset("obj", Path("foo"), "baz", nx=True, xx=True)


@pytest.mark.integrations
@pytest.mark.json
def test_mgetshouldsucceed(client):
    client.jsonset("1", Path.rootPath(), 1)
    client.jsonset("2", Path.rootPath(), 2)
    r = client.jsonmget(Path.rootPath(), "1", "2")
    e = [1, 2]
    assert e == r


@pytest.mark.integrations
@pytest.mark.json
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
def test_clearShouldSucceed(client):
    client.jsonset("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 1 == client.jsonclear("arr", Path.rootPath())
    assert [] == client.jsonget("arr")


@pytest.mark.integrations
@pytest.mark.json
def test_typeshouldsucceed(client):
    client.jsonset("1", Path.rootPath(), 1)
    assert b"integer" == client.jsontype("1")


@pytest.mark.integrations
@pytest.mark.json
def test_numincrbyshouldsucceed(client):
    client.jsonset("num", Path.rootPath(), 1)
    assert 2 == client.jsonnumincrby("num", Path.rootPath(), 1)
    assert 2.5 == client.jsonnumincrby("num", Path.rootPath(), 0.5)
    assert 1.25 == client.jsonnumincrby("num", Path.rootPath(), -1.25)


@pytest.mark.integrations
@pytest.mark.json
def test_nummultbyshouldsucceed(client):
    client.jsonset("num", Path.rootPath(), 1)
    assert 2 == client.jsonnummultby("num", Path.rootPath(), 2)
    assert 5 == client.jsonnummultby("num", Path.rootPath(), 2.5)
    assert 2.5 == client.jsonnummultby("num", Path.rootPath(), 0.5)


@pytest.mark.integrations
@pytest.mark.json
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
def test_toggleShouldSucceed(client):
    client.jsonset("bool", Path.rootPath(), False)
    print(client.jsontoggle("bool", Path.rootPath()))
    print(client.jsontoggle("bool", Path.rootPath()))
    assert client.jsontoggle("bool", Path.rootPath())
    assert not client.jsontoggle("bool", Path.rootPath())
    # check non-boolean value
    client.jsonset("num", Path.rootPath(), 1)
    with pytest.raises(redis.exceptions.ResponseError):
        client.jsontoggle("num", Path.rootPath())


@pytest.mark.integrations
@pytest.mark.json
def test_strappendshouldsucceed(client):
    client.jsonset("str", Path.rootPath(), "foo")
    assert 6 == client.jsonstrappend("str", "bar", Path.rootPath())
    assert "foobar" == client.jsonget("str", Path.rootPath())


@pytest.mark.integrations
@pytest.mark.json
def test_debug(client):
    client.jsonset("str", Path.rootPath(), "foo")
    assert 24 == client.jsondebug("str", Path.rootPath())


@pytest.mark.integrations
@pytest.mark.json
def test_strlenshouldsucceed(client):
    client.jsonset("str", Path.rootPath(), "foo")
    assert 3 == client.jsonstrlen("str", Path.rootPath())
    client.jsonstrappend("str", "bar", Path.rootPath())
    assert 6 == client.jsonstrlen("str", Path.rootPath())


@pytest.mark.integrations
@pytest.mark.json
def test_arrappendshouldsucceed(client):
    client.jsonset("arr", Path.rootPath(), [1])
    assert 2 == client.jsonarrappend("arr", Path.rootPath(), 2)
    assert 4 == client.jsonarrappend("arr", Path.rootPath(), 3, 4)
    assert 7 == client.jsonarrappend("arr", Path.rootPath(), *[5, 6, 7])


@pytest.mark.integrations
@pytest.mark.json
def testArrIndexShouldSucceed(client):
    client.jsonset("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 1 == client.jsonarrindex("arr", Path.rootPath(), 1)
    assert -1 == client.jsonarrindex("arr", Path.rootPath(), 1, 2)


@pytest.mark.integrations
@pytest.mark.json
def test_arrinsertshouldsucceed(client):
    client.jsonset("arr", Path.rootPath(), [0, 4])
    assert 5 - -client.jsonarrinsert(
        "arr",
        Path.rootPath(),
        1,
        *[
            1,
            2,
            3,
        ]
    )
    assert [0, 1, 2, 3, 4] == client.jsonget("arr")


@pytest.mark.integrations
@pytest.mark.json
def test_arrlenshouldsucceed(client):
    client.jsonset("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 5 == client.jsonarrlen("arr", Path.rootPath())


@pytest.mark.integrations
@pytest.mark.json
def test_arrpopshouldsucceed(client):
    client.jsonset("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 4 == client.jsonarrpop("arr", Path.rootPath(), 4)
    assert 3 == client.jsonarrpop("arr", Path.rootPath(), -1)
    assert 2 == client.jsonarrpop("arr", Path.rootPath())
    assert 0 == client.jsonarrpop("arr", Path.rootPath(), 0)
    assert [1] == client.jsonget("arr")


@pytest.mark.integrations
@pytest.mark.json
def test_arrtrimshouldsucceed(client):
    client.jsonset("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 3 == client.jsonarrtrim("arr", Path.rootPath(), 1, 3)
    assert [1, 2, 3] == client.jsonget("arr")


@pytest.mark.integrations
@pytest.mark.json
def test_respshouldsucceed(client):
    obj = {"foo": "bar", "baz": 1, "qaz": True}
    client.jsonset("obj", Path.rootPath(), obj)
    assert b"bar" == client.jsonresp("obj", Path("foo"))
    assert 1 == client.jsonresp("obj", Path("baz"))
    assert client.jsonresp("obj", Path("qaz"))


@pytest.mark.integrations
@pytest.mark.json
def test_objkeysshouldsucceed(client):
    obj = {"foo": "bar", "baz": "qaz"}
    client.jsonset("obj", Path.rootPath(), obj)
    keys = client.jsonobjkeys("obj", Path.rootPath())
    keys.sort()
    exp = list(obj.keys())
    exp.sort()
    assert exp == keys


@pytest.mark.integrations
@pytest.mark.json
def test_objlenshouldsucceed(client):
    obj = {"foo": "bar", "baz": "qaz"}
    client.jsonset("obj", Path.rootPath(), obj)
    assert len(obj) == client.jsonobjlen("obj", Path.rootPath())


@pytest.mark.integrations
@pytest.mark.pipeline
@pytest.mark.json
def test_pipelineshouldsucceed(client):
    p = client.pipeline()
    p.jsonset("foo", Path.rootPath(), "bar")
    p.jsonget("foo")
    p.jsondel("foo")
    p.exists("foo")
    assert [True, "bar", 1, False] == p.execute()
