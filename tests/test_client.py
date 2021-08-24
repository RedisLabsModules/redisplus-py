from redisplus import Client
from redis import Redis
from redis.client import Pipeline, bool_ok


def test_client_init():
    assert isinstance(Client().client, Redis)
    assert isinstance(Client(Redis()).client, Redis)

def test_client_command_mixin():
    c = Client()
    assert c.flushdb()

def test_client_executor():
    c = Client()
    assert c.execute_command("FLUSHDB")

# TODO test pipeline basics
def test_client_pipeline():
    c = Client()
    assert isinstance(c.pipeline(), Pipeline)

    with c.pipeline() as p:
        p.set("foo", "bar")
        bool_ok == p.execute()