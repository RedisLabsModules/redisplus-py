from abc import ABC
from redis.client import Pipeline
from redis.commands import Commands

class AbstractFeature(Commands, ABC):

    def execute_command(self, *args, **kwargs):
        """Execute redis command."""
        return self.client.execute_command(*args, **kwargs)

    @property
    def client(self):
        """Get the client instance."""
        return self.CLIENT

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for later execution.

        ``transaction`` indicates whether all commands should be executed atomically.
        Apart from making a group of operations atomic, pipelines are useful for reducing
        the back-and-forth overhead between the client and server.
        Overridden in order to provide the right client through the pipeline.
        """
        p = Pipeline(
            connection_pool=self.client.connection_pool,
            response_callbacks=self.client.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint,
        )
        return p
