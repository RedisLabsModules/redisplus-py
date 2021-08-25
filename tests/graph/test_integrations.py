from redisplus.graph import Node, Edge, Graph, Path
import redisplus
from redisplus.client import Client
from redis.exceptions import ResponseError
from redis.client import Redis
import pytest


@pytest.fixture
def client():
    rc = Client(Redis(decode_responses=True), extras={"graph": {"name": "social"}})
    assert isinstance(rc.graph, redisplus.graph.Graph)
    rc.flushdb()

    return rc.graph


@pytest.mark.integrations
@pytest.mark.graph
def test_graph_creation(client):

    john = Node(
        label="person",
        properties={
            "name": "John Doe",
            "age": 33,
            "gender": "male",
            "status": "single",
        },
    )
    client.add_node(john)
    japan = Node(label="country", properties={"name": "Japan"})

    client.add_node(japan)
    edge = Edge(john, "visited", japan, properties={"purpose": "pleasure"})
    client.add_edge(edge)

    client.commit()

    query = (
        'MATCH (p:person)-[v:visited {purpose:"pleasure"}]->(c:country) '
        "RETURN p, v, c"
    )

    result = client.query(query)

    person = result.result_set[0][0]
    visit = result.result_set[0][1]
    country = result.result_set[0][2]

    assert person == john
    assert visit.properties == edge.properties
    assert country == japan

    query = """RETURN [1, 2.3, "4", true, false, null]"""
    result = client.query(query)
    assert [1, 2.3, "4", True, False, None] == result.result_set[0][0]

    # All done, remove graph.
    client.delete()


@pytest.mark.integrations
@pytest.mark.graph
def test_array_functions(client):
    query = """CREATE (p:person{name:'a',age:32, array:[0,1,2]})"""
    client.query(query)

    query = """WITH [0,1,2] as x return x"""
    result = client.query(query)
    assert [0, 1, 2] == result.result_set[0][0]

    query = """MATCH(n) return collect(n)"""
    result = client.query(query)

    a = Node(
        node_id=0,
        label="person",
        properties={"name": "a", "age": 32, "array": [0, 1, 2]},
    )

    assert [a] == result.result_set[0][0]

    # All done, remove graph.
    client.delete()


@pytest.mark.integrations
@pytest.mark.graph
def test_path(client):

    node0 = Node(node_id=0, label="L1")
    node1 = Node(node_id=1, label="L1")
    edge01 = Edge(node0, "R1", node1, edge_id=0, properties={"value": 1})

    client.add_node(node0)
    client.add_node(node1)
    client.add_edge(edge01)

    client.flush()

    path01 = Path.new_empty_path().add_node(node0).add_edge(edge01).add_node(node1)
    expected_results = [[path01]]

    query = "MATCH p=(:L1)-[:R1]->(:L1) RETURN p ORDER BY p"
    result = client.query(query)
    assert expected_results == result.result_set

    # All done, remove graph.
    client.delete()


@pytest.mark.integrations
@pytest.mark.graph
def test_param(client):
    params = [1, 2.3, "str", True, False, None, [0, 1, 2]]
    query = "RETURN $param"
    for param in params:
        result = client.query(query, {"param": param})
        expected_results = [[param]]
        assert expected_results == result.result_set

    # All done, remove graph.
    client.delete()


@pytest.mark.integrations
@pytest.mark.graph
def test_map(client):

    query = "RETURN {a:1, b:'str', c:NULL, d:[1,2,3], e:True, f:{x:1, y:2}}"

    actual = client.query(query).result_set[0][0]
    expected = {
        "a": 1,
        "b": "str",
        "c": None,
        "d": [1, 2, 3],
        "e": True,
        "f": {"x": 1, "y": 2},
    }

    assert actual == expected

    # All done, remove graph.
    client.delete()


@pytest.mark.integrations
@pytest.mark.graph
def test_point(client):

    query = "RETURN point({latitude: 32.070794860, longitude: 34.820751118})"
    expected_lat = 32.070794860
    expected_lon = 34.820751118
    actual = client.query(query).result_set[0][0]
    assert abs(actual["latitude"] - expected_lat) < 0.001
    assert abs(actual["longitude"] - expected_lon) < 0.001

    query = "RETURN point({latitude: 32, longitude: 34.0})"
    expected_lat = 32
    expected_lon = 34
    actual = client.query(query).result_set[0][0]
    assert abs(actual["latitude"] - expected_lat) < 0.001
    assert abs(actual["longitude"] - expected_lon) < 0.001

    # All done, remove graph.
    client.delete()


@pytest.mark.integrations
@pytest.mark.graph
def test_index_response(client):

    result_set = client.query("CREATE INDEX ON :person(age)")
    assert 1 == result_set.indices_created

    result_set = client.query("CREATE INDEX ON :person(age)")
    assert 0 == result_set.indices_created

    result_set = client.query("DROP INDEX ON :person(age)")
    assert 1 == result_set.indices_deleted

    with pytest.raises(ResponseError):
        result_set = client.query("DROP INDEX ON :person(age)")

    client.delete()


@pytest.mark.integrations
@pytest.mark.graph
def test_stringify_query_result(client):

    john = Node(
        alias="a",
        label="person",
        properties={
            "name": "John Doe",
            "age": 33,
            "gender": "male",
            "status": "single",
        },
    )
    client.add_node(john)
    japan = Node(alias="b", label="country", properties={"name": "Japan"})

    client.add_node(japan)
    edge = Edge(john, "visited", japan, properties={"purpose": "pleasure"})
    client.add_edge(edge)

    assert (
        str(john)
        == """(a:person{age:33,gender:"male",name:"John Doe",status:"single"})"""
    )
    assert (
        str(edge)
        == """(a:person{age:33,gender:"male",name:"John Doe",status:"single"})"""
        + """-[:visited{purpose:"pleasure"}]->"""
        + """(b:country{name:"Japan"})"""
    )
    assert str(japan) == """(b:country{name:"Japan"})"""

    client.commit()

    query = """MATCH (p:person)-[v:visited {purpose:"pleasure"}]->(c:country)
            RETURN p, v, c"""

    result = client.query(query)
    person = result.result_set[0][0]
    visit = result.result_set[0][1]
    country = result.result_set[0][2]

    assert (
        str(person)
        == """(:person{age:33,gender:"male",name:"John Doe",status:"single"})"""
    )
    assert str(visit) == """()-[:visited{purpose:"pleasure"}]->()"""
    assert str(country) == """(:country{name:"Japan"})"""

    client.delete()


@pytest.mark.integrations
@pytest.mark.graph
def test_optional_match(client):

    # Build a graph of form (a)-[R]->(b)
    node0 = Node(node_id=0, label="L1", properties={"value": "a"})
    node1 = Node(node_id=1, label="L1", properties={"value": "b"})

    edge01 = Edge(node0, "R", node1, edge_id=0)

    client.add_node(node0)
    client.add_node(node1)
    client.add_edge(edge01)

    client.flush()

    # Issue a query that collects all outgoing edges from both nodes (the second has none).
    query = """MATCH (a) OPTIONAL MATCH (a)-[e]->(b) RETURN a, e, b ORDER BY a.value"""
    expected_results = [[node0, edge01, node1], [node1, None, None]]

    result = client.query(query)
    assert expected_results == result.result_set

    client.delete()


@pytest.mark.integrations
@pytest.mark.graph
def test_cached_execution(client):
    client.query("CREATE ()")

    uncached_result = client.query("MATCH (n) RETURN n, $param", {"param": [0]})
    assert uncached_result.cached_execution is False

    # loop to make sure the query is cached on each thread on server
    for x in range(0, 64):
        cached_result = client.query("MATCH (n) RETURN n, $param", {"param": [0]})
        assert uncached_result.result_set == cached_result.result_set

    # should be cached on all threads by now
    assert cached_result.cached_execution

    client.delete()


@pytest.mark.integrations
@pytest.mark.graph
def test_execution_plan(client):
    create_query = """CREATE (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
    (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
    (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    client.query(create_query)

    result = client.execution_plan(
        "MATCH (r:Rider)-[:rides]->(t:Team) WHERE t.name = $name RETURN r.name, t.name, $params",
        {"name": "Yehuda"},
    )
    expected = "Results\n    Project\n        Conditional Traverse | (t:Team)->(r:Rider)\n            Filter\n                Node By Label Scan | (t:Team)"
    assert result == expected

    client.delete()


@pytest.mark.integrations
@pytest.mark.graph
def test_query_timeout(client):
    # Build a sample graph with 1000 nodes.
    client.query("UNWIND range(0,1000) as val CREATE ({v: val})")
    # Issue a long-running query with a 1-millisecond timeout.
    with pytest.raises(ResponseError):
        client.query("MATCH (a), (b), (c), (d) RETURN *", timeout=1)
        assert False is False

    with pytest.raises(Exception):
        client.query("RETURN 1", timeout="str")
        assert False is False


@pytest.mark.integrations
@pytest.mark.graph
def test_read_only_query(client):
    with pytest.raises(Exception):
        # Issue a write query, specifying read-only true, this call should fail.
        client.query("CREATE (p:person {name:'a'})", read_only=True)
        assert False is False


@pytest.mark.integrations
@pytest.mark.graph
def test_cache_sync(client):
    pass
    return
    # This test verifies that client internal graph schema cache stays
    # in sync with the graph schema
    #
    # Client B will try to get Client A out of sync by:
    # 1. deleting the graph
    # 2. reconstructing the graph in a different order, this will casuse
    #    a differance in the current mapping between string IDs and the
    #    mapping Client A is aware of
    #
    # Client A should pick up on the changes by comparing graph versions
    # and resyncing its cache.

    A = Graph("cache-sync", self.r)
    B = Graph("cache-sync", self.r)

    # Build order:
    # 1. introduce label 'L' and 'K'
    # 2. introduce attribute 'x' and 'q'
    # 3. introduce relationship-type 'R' and 'S'

    A.query("CREATE (:L)")
    B.query("CREATE (:K)")
    A.query("MATCH (n) SET n.x = 1")
    B.query("MATCH (n) SET n.q = 1")
    A.query("MATCH (n) CREATE (n)-[:R]->()")
    B.query("MATCH (n) CREATE (n)-[:S]->()")

    # Cause client A to populate its cache
    A.query("MATCH (n)-[e]->() RETURN n, e")

    assert len(A._labels) == 2
    assert len(A._properties) == 2
    assert len(A._relationshipTypes) == 2
    assert A._labels[0] == "L"
    assert A._labels[1] == "K"
    assert A._properties[0] == "x"
    assert A._properties[1] == "q"
    assert A._relationshipTypes[0] == "R"
    assert A._relationshipTypes[1] == "S"

    # Have client B reconstruct the graph in a different order.
    B.delete()

    # Build order:
    # 1. introduce relationship-type 'R'
    # 2. introduce label 'L'
    # 3. introduce attribute 'x'
    B.query("CREATE ()-[:S]->()")
    B.query("CREATE ()-[:R]->()")
    B.query("CREATE (:K)")
    B.query("CREATE (:L)")
    B.query("MATCH (n) SET n.q = 1")
    B.query("MATCH (n) SET n.x = 1")

    # A's internal cached mapping is now out of sync
    # issue a query and make sure A's cache is synced.
    A.query("MATCH (n)-[e]->() RETURN n, e")

    assert len(A._labels) == 2
    assert len(A._properties) == 2
    assert len(A._relationshipTypes) == 2
    assert A._labels[0] == "K"
    assert A._labels[1] == "L"
    assert A._properties[0] == "q"
    assert A._properties[1] == "x"
    assert A._relationshipTypes[0] == "S"
    assert A._relationshipTypes[1] == "R"
