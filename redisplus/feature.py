from abc import ABC
from redis.client import Pipeline


class AbstractFeature(ABC):
    """AbstractBase for all client features.

    This class ensures that we can contstruct modules that access
    the appropriate internal redis client objects.
    """

    def execute_command(self, *args, **kwargs):
        """Execute redis command."""
        return self.__client__.execute_command(*args, **kwargs)

    @property
    def __client__(self):
        """Get the client instance set by the redis module class."""
        return self.client

    def _pipeline(self, **kwargs):
        """Build and return a pipeline object.
        By implementing a pipeline, the individual
        features receive a working pipeline, with customizable
        characteristics, if necessary.

        cls - If passed in as kwargs this is the command class,
                to mix into the pipeline. Otherwise self.commandmixins is used.

        _ - Any kwargs prefixed with an underscore (_) will be used after
        initialization of the redis-py pipeline object, and setattr on
        the generated pipeline.
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
        cls = kwargs.get("cls", self.commandmixin)
        if "cls" in kwargs.keys():
            kwargs.pop("cls")

        internals = {}
        sanitized = {}
        for k, v in kwargs.items():
            if k.find("_") == 0:
                internals[k] = kwargs[k]
            else:
                sanitized[k] = kwargs[k]

        # construct an internal class (Piper) that is effectively as redis
        # pipeline, and ensure we mix in, the commands for the associated module
        class Piper(cls, Pipeline):
            pass

        p = Piper(**sanitized)
        for k, v in internals.items():
            setattr(p, k, v)
        return p

    def pipeline(self, **kwargs):
        """Returns the pipline instance we create.
        This allows individual module clients to support pipelines directly,
        by setting self.commandmixin to the CommandMixin used, thereby adding
        support to the pipeline.

        If no commandmixin is set on the base class, self._pipeline, which is
        called, will look for kwargs['cls'].
        """
        return self._pipeline(**kwargs)
