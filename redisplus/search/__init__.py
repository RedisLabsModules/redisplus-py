from redis.client import Pipeline, Redis

from .commands import CommandMixin
from ..feature import AbstractFeature


class Search(CommandMixin, AbstractFeature, object):
    """
    Create a client for talking to search.
    It abstracts the API of the module and lets you just use the engine.
    """

    class BatchIndexer(object):
        """
        A batch indexer allows you to automatically batch
        document indexing in pipelines, flushing it every N documents.
        """

        def __init__(self, client, chunk_size=1000):

            self.client = client
            self.pipeline = client.pipeline(False)
            self.total = 0
            self.chunk_size = chunk_size
            self.current_chunk = 0

        def __del__(self):
            if self.current_chunk:
                self.commit()

        def add_document(
            self,
            doc_id,
            nosave=False,
            score=1.0,
            payload=None,
            replace=False,
            partial=False,
            no_create=False,
            **fields
        ):
            """
            Add a document to the batch query
            """
            self.client._add_document(
                doc_id,
                conn=self.pipeline,
                nosave=nosave,
                score=score,
                payload=payload,
                replace=replace,
                partial=partial,
                no_create=no_create,
                **fields
            )
            self.current_chunk += 1
            self.total += 1
            if self.current_chunk >= self.chunk_size:
                self.commit()

        def add_document_hash(
            self,
            doc_id,
            score=1.0,
            replace=False,
        ):
            """
            Add a hash to the batch query
            """
            self.client._add_document_hash(
                doc_id,
                conn=self.pipeline,
                score=score,
                replace=replace,
            )
            self.current_chunk += 1
            self.total += 1
            if self.current_chunk >= self.chunk_size:
                self.commit()

        def commit(self):
            """
            Manually commit and flush the batch indexing query
            """
            self.pipeline.execute()
            self.current_chunk = 0

    def __init__(self, client: Redis, index_name: str = "idx"):
        """
        Create a new Client for the given index_name.
        The default name is `idx`

        If conn is not None, we employ an already existing redis connection
        """
        self.client = client
        self.index_name = index_name
        self.commandmixin = CommandMixin

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.

        Overridden in order to provide the right client through the pipeline.
        """
        p = Pipeline(
            connection_pool=self.client.connection_pool,
            response_callbacks=self.client.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint,
        )
        return p


class Pipeline(Pipeline):
    """Pipeline for client"""

    def pipeline(self):
        raise AttributeError("Pipelines cannot be created within pipelines.")
        self.commandmixin = CommandMixin
