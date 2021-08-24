from abc import ABC
from redis.client import Pipeline


class AbstractFeature(ABC):
    """AbstractBase for all client features.

    This class ensures that we can contstruct modules that access
    the appropriate internal redis client objects.
    """

    def execute_command(self, *args, **kwargs):
        """Execute redis command."""
        return self.client.execute_command(*args, **kwargs)

    @property
    def client(self):
        """Get the client instance."""
        return self.CLIENT

    def _pipeline(self, cls, **kwargs):
        """Build and return a pipeline object.
        By implementing a pipeline, the individual
        features receive a working pipeline, with customizable
        characteristics, if necessary.

        cls - This is the command class, to mix into the pipeline.
        """
        # set some sane kwargs from common defaults for the pipeline
        kwargs["transaction"] = kwargs.get("transaction", True)
        kwargs["shard_hint"] = kwargs.get("shard_hint", None)
        kwargs["connection_pool"] = kwargs.get(
            "connection_pool", self.client.connection_pool
        )
        kwargs["response_callbacks"] = kwargs.get(
            "response_callbacks", self.client.response_callbacks
        )

        internals = {}
        sanitized = {}
        for k, v in kwargs.items():
            if k.find("_") == 0:
                internals[k] = kwargs[k]
            else:
                sanitized[k] = kwargs[k]

        class Piper(Pipeline, cls):
            pass

        p = Piper(**sanitized)
        for k, v in internals.items():
            setattr(p, k, v)
        return p

    def pipeline(self, **kwargs):
        """
        Implement in child classes, to enable a pipeline, with optional feature
        specific arguments, as needed.  Any arguments passed in prefixed with an
        underscore (_) will be setattr-ed on the pipeline object after creation,
        returning an instance of Pipeline with those objects included.

        The child class must call _pipeline, and provide the Commands mixin.
        """
        raise NotImplementedError("Pipelines must be implemented  by children.")
