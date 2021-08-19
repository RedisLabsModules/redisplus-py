class CommandMixin:
    def delete(self):
        """
        Deletes graph.
        """
        self._clear_schema()
        return self.execute_command("GRAPH.DELETE", self.name)

    # declared here, to override the built in redis.db.flush()
    def flush(self):
        """
        Commit the graph and reset the edges and nodes to zero length
        """
        self.commit()
        self.nodes = {}
        self.edges = []

    def execution_plan(self, query, params=None):
        """
        Get the execution plan for given query,
        GRAPH.EXPLAIN returns an array of operations.

        Args:
            query: the query that will be executed
            params: query parameters
        """
        if params is not None:
            query = self._build_params_header(params) + query

        plan = self.execute_command("GRAPH.EXPLAIN", self.name, query)
        return "\n".join(plan)
