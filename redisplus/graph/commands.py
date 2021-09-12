from redis import DataError


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

    def explain(self, query, params=None):
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

    def slowlog(self):
        """
        Get a list containing up to 10 of the slowest queries issued against the given graph ID.

        Each item in the list has the following structure:
        1. A unix timestamp at which the log entry was processed.
        2. The issued command.
        3. The issued query.
        4. The amount of time needed for its execution, in milliseconds.
        """
        return self.execute_command("GRAPH.SLOWLOG", self.name)

    def config(self, name, value=None, set=False):
        """
        Retrieve or update a RedisGraph configuration.

        Args:
            name: The name of the configuration
            value: The value we want to ser (can be used only when ``set`` is on)
            set: turn on to set a configuration. Default behavior is get.
        """
        params = ["SET" if set else "GET", name]
        if value is not None:
            if set:
                params.append(value)
            else:
                raise DataError("``value`` can be provided only when ``set`` is True")
        return self.execute_command("GRAPH.CONFIG", *params)
