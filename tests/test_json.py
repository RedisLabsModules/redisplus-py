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
    return rc


@pytest.mark.integrations
@pytest.mark.json
def test_json_setbinarykey(client):
    d = {"hello": "world", b"some": "value"}
    with pytest.raises(TypeError):
        client.json.jsonset("somekey", Path.rootPath(), d)
    assert client.json.jsonset("somekey", Path.rootPath(), d, decode_keys=True)


@pytest.mark.integrations
@pytest.mark.json
def test_json_setgetdeleteforget(client):
    assert client.json.jsonset("foo", Path.rootPath(), "bar")
    assert client.json.jsonget("foo") == "bar"
    assert client.json.jsonget("baz") is None
    assert client.json.jsondel("foo") == 1
    assert client.json.jsonforget("foo") == 0  # second delete
    assert client.exists("foo") == 0


@pytest.mark.integrations
@pytest.mark.json
def test_justaget(client):
    client.json.jsonset("foo", Path.rootPath(), "bar")
    assert client.json.jsonget("foo") == "bar"


@pytest.mark.integrations
@pytest.mark.json
def test_json_get_jset(client):
    assert client.json.jsonset("foo", Path.rootPath(), "bar")
    assert "bar" == client.json.jsonget("foo")
    assert None == client.json.jsonget("baz")
    assert 1 == client.json.jsondel("foo")
    assert client.exists("foo") == 0


@pytest.mark.integrations
@pytest.mark.json
def test_nonascii_setgetdelete(client):
    assert client.json.jsonset("notascii", Path.rootPath(), "hyvää-élève") is True
    assert "hyvää-élève" == client.json.jsonget("notascii", no_escape=True)
    assert 1 == client.json.jsondel("notascii")
    assert client.exists("notascii") == 0


@pytest.mark.integrations
@pytest.mark.json
def test_jsonsetexistentialmodifiersshouldsucceed(client):
    obj = {"foo": "bar"}
    assert client.json.jsonset("obj", Path.rootPath(), obj)

    # Test that flags prevent updates when conditions are unmet
    assert client.json.jsonset("obj", Path("foo"), "baz", nx=True) is None
    assert client.json.jsonset("obj", Path("qaz"), "baz", xx=True) is None

    # Test that flags allow updates when conditions are met
    assert client.json.jsonset("obj", Path("foo"), "baz", xx=True)
    assert client.json.jsonset("obj", Path("qaz"), "baz", nx=True)

    # Test that flags are mutually exlusive
    with pytest.raises(Exception):
        client.json.jsonset("obj", Path("foo"), "baz", nx=True, xx=True)


@pytest.mark.integrations
@pytest.mark.json
def test_mgetshouldsucceed(client):
    client.json.jsonset("1", Path.rootPath(), 1)
    client.json.jsonset("2", Path.rootPath(), 2)
    r = client.json.jsonmget(Path.rootPath(), "1", "2")
    e = [1, 2]
    assert e == r


@pytest.mark.integrations
@pytest.mark.json
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
def test_clearShouldSucceed(client):
    client.json.jsonset("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 1 == client.json.jsonclear("arr", Path.rootPath())
    assert [] == client.json.jsonget("arr")


@pytest.mark.integrations
@pytest.mark.json
def test_typeshouldsucceed(client):
    client.json.jsonset("1", Path.rootPath(), 1)
    assert b"integer" == client.json.jsontype("1")


@pytest.mark.integrations
@pytest.mark.json
def test_numincrbyshouldsucceed(client):
    client.json.jsonset("num", Path.rootPath(), 1)
    assert 2 == client.json.jsonnumincrby("num", Path.rootPath(), 1)
    assert 2.5 == client.json.jsonnumincrby("num", Path.rootPath(), 0.5)
    assert 1.25 == client.json.jsonnumincrby("num", Path.rootPath(), -1.25)


@pytest.mark.integrations
@pytest.mark.json
def test_nummultbyshouldsucceed(client):
    client.json.jsonset("num", Path.rootPath(), 1)
    assert 2 == client.json.jsonnummultby("num", Path.rootPath(), 2)
    assert 5 == client.json.jsonnummultby("num", Path.rootPath(), 2.5)
    assert 2.5 == client.json.jsonnummultby("num", Path.rootPath(), 0.5)


@pytest.mark.integrations
@pytest.mark.json
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
def test_toggleShouldSucceed(client):
    client.json.jsonset("bool", Path.rootPath(), False)
    print(client.json.jsontoggle("bool", Path.rootPath()))
    print(client.json.jsontoggle("bool", Path.rootPath()))
    assert client.json.jsontoggle("bool", Path.rootPath())
    assert not client.json.jsontoggle("bool", Path.rootPath())
    # check non-boolean value
    client.json.jsonset("num", Path.rootPath(), 1)
    with pytest.raises(redis.exceptions.ResponseError):
        client.json.jsontoggle("num", Path.rootPath())


@pytest.mark.integrations
@pytest.mark.json
def test_strappendshouldsucceed(client):
    client.json.jsonset("str", Path.rootPath(), "foo")
    assert 6 == client.json.jsonstrappend("str", "bar", Path.rootPath())
    assert "foobar" == client.json.jsonget("str", Path.rootPath())


@pytest.mark.integrations
@pytest.mark.json
def test_debug(client):
    client.json.jsonset("str", Path.rootPath(), "foo")
    assert 24 == client.json.jsondebug("str", Path.rootPath())


@pytest.mark.integrations
@pytest.mark.json
def test_strlenshouldsucceed(client):
    client.json.jsonset("str", Path.rootPath(), "foo")
    assert 3 == client.json.jsonstrlen("str", Path.rootPath())
    client.json.jsonstrappend("str", "bar", Path.rootPath())
    assert 6 == client.json.jsonstrlen("str", Path.rootPath())


@pytest.mark.integrations
@pytest.mark.json
def test_arrappendshouldsucceed(client):
    client.json.jsonset("arr", Path.rootPath(), [1])
    assert 2 == client.json.jsonarrappend("arr", Path.rootPath(), 2)
    assert 4 == client.json.jsonarrappend("arr", Path.rootPath(), 3, 4)
    assert 7 == client.json.jsonarrappend("arr", Path.rootPath(), *[5, 6, 7])


@pytest.mark.integrations
@pytest.mark.json
def testArrIndexShouldSucceed(client):
    client.json.jsonset("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 1 == client.json.jsonarrindex("arr", Path.rootPath(), 1)
    assert -1 == client.json.jsonarrindex("arr", Path.rootPath(), 1, 2)


@pytest.mark.integrations
@pytest.mark.json
def test_arrinsertshouldsucceed(client):
    client.json.jsonset("arr", Path.rootPath(), [0, 4])
    assert 5 - -client.json.jsonarrinsert(
        "arr",
        Path.rootPath(),
        1,
        *[
            1,
            2,
            3,
        ]
    )
    assert [0, 1, 2, 3, 4] == client.json.jsonget("arr")


@pytest.mark.integrations
@pytest.mark.json
def test_arrlenshouldsucceed(client):
    client.json.jsonset("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 5 == client.json.jsonarrlen("arr", Path.rootPath())


@pytest.mark.integrations
@pytest.mark.json
def test_arrpopshouldsucceed(client):
    client.json.jsonset("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 4 == client.json.jsonarrpop("arr", Path.rootPath(), 4)
    assert 3 == client.json.jsonarrpop("arr", Path.rootPath(), -1)
    assert 2 == client.json.jsonarrpop("arr", Path.rootPath())
    assert 0 == client.json.jsonarrpop("arr", Path.rootPath(), 0)
    assert [1] == client.json.jsonget("arr")


@pytest.mark.integrations
@pytest.mark.json
def test_arrtrimshouldsucceed(client):
    client.json.jsonset("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 3 == client.json.jsonarrtrim("arr", Path.rootPath(), 1, 3)
    assert [1, 2, 3] == client.json.jsonget("arr")


@pytest.mark.integrations
@pytest.mark.json
def test_respshouldsucceed(client):
    obj = {"foo": "bar", "baz": 1, "qaz": True}
    client.json.jsonset("obj", Path.rootPath(), obj)
    assert b"bar" == client.json.jsonresp("obj", Path("foo"))
    assert 1 == client.json.jsonresp("obj", Path("baz"))
    assert client.json.jsonresp("obj", Path("qaz"))


@pytest.mark.integrations
@pytest.mark.json
def test_objkeysshouldsucceed(client):
    obj = {"foo": "bar", "baz": "qaz"}
    client.json.jsonset("obj", Path.rootPath(), obj)
    keys = client.json.jsonobjkeys("obj", Path.rootPath())
    keys.sort()
    exp = list(obj.keys())
    exp.sort()
    assert exp == keys


@pytest.mark.integrations
@pytest.mark.json
def test_objlenshouldsucceed(client):
    obj = {"foo": "bar", "baz": "qaz"}
    client.json.jsonset("obj", Path.rootPath(), obj)
    assert len(obj) == client.json.jsonobjlen("obj", Path.rootPath())


@pytest.mark.integrations
@pytest.mark.pipeline
@pytest.mark.json
def test_pipelineshouldsucceed(client):
    p = client.json.pipeline()
    p.jsonset("foo", Path.rootPath(), "bar")
    p.jsonget("foo")
    p.jsondel("foo")
    assert [True, "bar", 1] == p.execute()
    assert client.exists("foo") == False
