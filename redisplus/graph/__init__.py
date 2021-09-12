from .node import Node  # noqa
from .edge import Edge  # noqa
from .path import Path  # noqa

from redis.client import Redis
from redis.exceptions import ResponseError
from ..feature import AbstractFeature
from ..helpers import random_string, quote_string
from .commands import CommandMixin
from .exceptions import VersionMismatchException
from .query_result import QueryResult


class Graph(CommandMixin, AbstractFeature, object):
    """
    Graph, collection of nodes and edges.
    """

    def __init__(self, client: Redis, name=random_string()):
        """
        Create a new graph.
        """
        self.NAME = name  # Graph key
        self.client = client

        self.nodes = {}
        self.edges = []
        self._labels = []  # List of node labels.
        self._properties = []  # List of properties.
        self._relationshipTypes = []  # List of relation types.
        self.version = 0  # Graph version

    @property
    def name(self):
        return self.NAME

    def _clear_schema(self):
        self._labels = []
        self._properties = []
        self._relationshipTypes = []

    def _refresh_schema(self):
        self._clear_schema()
        self._refresh_labels()
        self._refresh_relations()
        self._refresh_attributes()

    def _refresh_labels(self):
        lbls = self.labels()

        # Unpack data.
        self._labels = [None] * len(lbls)
        for i, l in enumerate(lbls):
            self._labels[i] = l[0]

    def _refresh_relations(self):
        rels = self.relationshipTypes()

        # Unpack data.
        self._relationshipTypes = [None] * len(rels)
        for i, r in enumerate(rels):
            self._relationshipTypes[i] = r[0]

    def _refresh_attributes(self):
        props = self.propertyKeys()

        # Unpack data.
        self._properties = [None] * len(props)
        for i, p in enumerate(props):
            self._properties[i] = p[0]

    def get_label(self, idx):
        """
        Returns a label by it's index

        Args:
            idx: The index of the label
        """
        try:
            label = self._labels[idx]
        except IndexError:
            # Refresh labels.
            self._refresh_labels()
            label = self._labels[idx]
        return label

    def get_relation(self, idx):
        """
        Returns a relationship type by it's index

        Args:
            idx: The index of the relation
        """
        try:
            relationship_type = self._relationshipTypes[idx]
        except IndexError:
            # Refresh relationship types.
            self._refresh_relations()
            relationship_type = self._relationshipTypes[idx]
        return relationship_type

    def get_property(self, idx):
        """
        Returns a property by it's index

        Args:
            idx: The index of the property
        """
        try:
            propertie = self._properties[idx]
        except IndexError:
            # Refresh properties.
            self._refresh_attributes()
            propertie = self._properties[idx]
        return propertie

    def add_node(self, node):
        """
        Adds a node to the graph.
        """
        if node.alias is None:
            node.alias = random_string()
        self.nodes[node.alias] = node

    def add_edge(self, edge):
        """
        Adds an edge to the graph.
        """
        if not (self.nodes[edge.src_node.alias] and self.nodes[edge.dest_node.alias]):
            raise AssertionError("Both edge's end must be in the graph")

        self.edges.append(edge)

    def commit(self):
        """
        Create entire graph.
        """
        if len(self.nodes) == 0 and len(self.edges) == 0:
            return None

        query = "CREATE "
        for _, node in self.nodes.items():
            query += str(node) + ","

        query += ",".join([str(edge) for edge in self.edges])

        # Discard leading comma.
        if query[-1] == ",":
            query = query[:-1]

        return self.query(query)

    def _build_params_header(self, params):
        if not isinstance(params, dict):
            raise TypeError("'params' must be a dict")
        # Header starts with "CYPHER"
        params_header = "CYPHER "
        for key, value in params.items():
            # If value is string add quotation marks.
            if isinstance(value, str):
                value = quote_string(value)
            # Value is None, replace with "null" string.
            elif value is None:
                value = "null"
            params_header += str(key) + "=" + str(value) + " "
        return params_header

    def query(self, q, params=None, timeout=None, read_only=False):
        """
        Executes a query against the graph.

        Args:
            q: the query
            params: query parameters
            timeout: maximum runtime for read queries in milliseconds
            read_only: executes a readonly query if set to True
        """

        # maintain original 'q'
        query = q

        # handle query parameters
        if params is not None:
            query = self._build_params_header(params) + query

        # construct query command
        # ask for compact result-set format
        # specify known graph version
        cmd = "GRAPH.RO_QUERY" if read_only else "GRAPH.QUERY"
        command = [cmd, self.name, query, "--compact"]

        # include timeout is specified
        if timeout:
            if not isinstance(timeout, int):
                raise Exception("Timeout argument must be a positive integer")
            command += ["timeout", timeout]

        # issue query
        try:
            response = self.execute_command(*command)
            return QueryResult(self, response)
        except ResponseError as e:
            if "wrong number of arguments" in str(e):
                print("Note: RedisGraph Python requires server version 2.2.8 or above")
            if "unknown command" in str(e) and read_only:
                # `GRAPH.RO_QUERY` is unavailable in older versions.
                return self.query(q, params, timeout, read_only=False)
            raise e
        except VersionMismatchException as e:
            # client view over the graph schema is out of sync
            # set client version and refresh local schema
            self.version = e.version
            self._refresh_schema()
            # re-issue query
            return self.query(q, params, timeout, read_only)

    def merge(self, pattern):
        """
        Merge pattern.
        """

        query = "MERGE "
        query += str(pattern)

        return self.query(query)

    # Procedures.
    def call_procedure(self, procedure, *args, read_only=False, **kwagrs):
        args = [quote_string(arg) for arg in args]
        q = "CALL %s(%s)" % (procedure, ",".join(args))

        y = kwagrs.get("y", None)
        if y:
            q += " YIELD %s" % ",".join(y)

        return self.query(q, read_only=read_only)

    def labels(self):
        return self.call_procedure("db.labels", read_only=True).result_set

    def relationshipTypes(self):
        return self.call_procedure("db.relationshipTypes", read_only=True).result_set

    def propertyKeys(self):
        return self.call_procedure("db.propertyKeys", read_only=True).result_set
