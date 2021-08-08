from ..utils import nativestr
from .commands import CommandMixin
from redis.commands import Commands as RedisCommands
from redis import Redis, ConnectionPool


class Client(CommandMixin, RedisCommands, object):
    """
    A client for the RediSearch module.
    It abstracts the API of the module and lets you just use the engine.
    """

    NUMERIC = "NUMERIC"

    CREATE_CMD = "FT.CREATE"
    ALTER_CMD = "FT.ALTER"
    SEARCH_CMD = "FT.SEARCH"
    ADD_CMD = "FT.ADD"
    ADDHASH_CMD = "FT.ADDHASH"
    DROP_CMD = "FT.DROP"
    EXPLAIN_CMD = "FT.EXPLAIN"
    DEL_CMD = "FT.DEL"
    AGGREGATE_CMD = "FT.AGGREGATE"
    CURSOR_CMD = "FT.CURSOR"
    SPELLCHECK_CMD = "FT.SPELLCHECK"
    DICT_ADD_CMD = "FT.DICTADD"
    DICT_DEL_CMD = "FT.DICTDEL"
    DICT_DUMP_CMD = "FT.DICTDUMP"
    GET_CMD = "FT.GET"
    MGET_CMD = "FT.MGET"
    CONFIG_CMD = "FT.CONFIG"
    TAGVALS_CMD = "FT.TAGVALS"
    ALIAS_ADD_CMD = "FT.ALIASADD"
    ALIAS_UPDATE_CMD = "FT.ALIASUPDATE"
    ALIAS_DEL_CMD = "FT.ALIASDEL"

    NOOFFSETS = "NOOFFSETS"
    NOFIELDS = "NOFIELDS"
    STOPWORDS = "STOPWORDS"

    class BatchIndexer(object):
        """
        A batch indexer allows you to automatically batch
        document indexing in pipelines, flushing it every N documents.
        """

        def __init__(self, client, chunk_size=1000):

            self.client = client
            self.pipeline = client.redis.pipeline(False)
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

    def __init__(
        self,
        index_name,
        host="localhost",
        port=6379,
        conn=None,
        password=None,
        decode_responses=True,
    ):
        """
        Create a new Client for the given index_name, and optional host and port

        If conn is not None, we employ an already existing redis connection
        """

        self.index_name = index_name

        self.redis = (
            conn
            if conn is not None
            else Redis(
                connection_pool=ConnectionPool(
                    host=host,
                    port=port,
                    password=password,
                    decode_responses=decode_responses,
                )
            )
        )
