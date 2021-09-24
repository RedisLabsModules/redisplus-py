import itertools
import time
import six

from .document import Document
from .result import Result
from .query import Query
from ._util import to_string
from .aggregation import AggregateRequest, AggregateResult, Cursor

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
INFO_CMD = "FT.INFO"

NOOFFSETS = "NOOFFSETS"
NOFIELDS = "NOFIELDS"
STOPWORDS = "STOPWORDS"


class CommandMixin:
    """Search commands."""

    def batch_indexer(self, chunk_size=100):
        """
        Create a new batch indexer from the client with a given chunk size.
        """
        return self.BatchIndexer(self, chunk_size=chunk_size)

    def create_index(
        self,
        fields,
        no_term_offsets=False,
        no_field_flags=False,
        stopwords=None,
        definition=None,
    ):
        """
        Create the search index. The index must not already exist.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftcreate>`_.

        Parameters:

        fields : list of search.Field
            a list of TextField or NumericField objects.
        no_term_offsets : bool
            If true, we will not save term offsets in the index.
        no_field_flags : bool
            If true, we will not save field flags that allow searching in specific fields.
        stopwords : list, tuple or set of str
            If not None, we create the index with this custom stopword list. The list can be empty.
        definition : search.IndexDefinition
            IndexDefinition instance that defines the index.
        """

        args = [CREATE_CMD, self.index_name]
        if definition is not None:
            args += definition.args
        if no_term_offsets:
            args.append(NOOFFSETS)
        if no_field_flags:
            args.append(NOFIELDS)
        if stopwords is not None and isinstance(stopwords, (list, tuple, set)):
            args += [STOPWORDS, len(stopwords)]
            if len(stopwords) > 0:
                args += list(stopwords)

        args.append("SCHEMA")
        args += list(itertools.chain(*(f.redis_args() for f in fields)))

        return self.execute_command(*args)

    def alter_schema_add(self, fields):
        """
        Alter the existing search index by adding new fields. The index must already exist.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftalter_schema_add>`_.

        Parameters:

        fields : list of search.Field
            A list of Field objects to add for the index.
        """

        args = [ALTER_CMD, self.index_name, "SCHEMA", "ADD"]
        args += list(itertools.chain(*(f.redis_args() for f in fields)))

        return self.execute_command(*args)

    def dropindex(self, delete_documents=False):
        """
        Drop the index if it exists.
        Default behavior was changed to not delete the indexed documents.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftdrop>`_.

        Parameters:

        delete_documents : bool
            If `True`, all documents will be deleted.
        """
        keep_str = "" if delete_documents else "KEEPDOCS"
        return self.execute_command(DROP_CMD, self.index_name, keep_str)

    def _add_document(
        self,
        doc_id,
        conn=None,
        nosave=False,
        score=1.0,
        payload=None,
        replace=False,
        partial=False,
        language=None,
        no_create=False,
        **fields
    ):
        """
        Internal add_document used for both batch and single doc indexing
        """
        if conn is None:
            conn = self.client

        if partial or no_create:
            replace = True

        args = [ADD_CMD, self.index_name, doc_id, score]
        if nosave:
            args.append("NOSAVE")
        if payload is not None:
            args.append("PAYLOAD")
            args.append(payload)
        if replace:
            args.append("REPLACE")
            if partial:
                args.append("PARTIAL")
            if no_create:
                args.append("NOCREATE")
        if language:
            args += ["LANGUAGE", language]
        args.append("FIELDS")
        args += list(itertools.chain(*fields.items()))
        return conn.execute_command(*args)

    def _add_document_hash(
        self,
        doc_id,
        conn=None,
        score=1.0,
        language=None,
        replace=False,
    ):
        """
        Internal add_document_hash used for both batch and single doc indexing
        """
        if conn is None:
            conn = self.client

        args = [ADDHASH_CMD, self.index_name, doc_id, score]

        if replace:
            args.append("REPLACE")

        if language:
            args += ["LANGUAGE", language]

        return conn.execute_command(*args)

    def add_document(
        self,
        doc_id,
        nosave=False,
        score=1.0,
        payload=None,
        replace=False,
        partial=False,
        language=None,
        no_create=False,
        **fields
    ):
        """
        Add a single document to the index.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftadd>`_.

        Parameters:

        doc_id :
            The id of the saved document.
        nosave : bool
            If set to true, we just index the document, and don't save a copy of it.
            This means that searches will just return ids.
        score : float
            The document ranking, between 0.0 and 1.0.
        payload :
            Optional inner-index payload we can save for fast access in scoring functions.
        replace : bool
            If True, and the document already is in the index, we perform an update and reindex the document
        partial : bool
            If True, the fields specified will be added to the existing document.
            This has the added benefit that any fields specified with `no_index`
            will not be reindexed again. Implies `replace`.
        language : str
            Specify the language used for document tokenization.
        no_create : bool
            If True, the document is only updated and reindexed if it already exists.
            If the document does not exist, an error will be returned. Implies `replace`.
        fields : kwargs
            kwargs dictionary of the document fields to be saved and/or indexed.
            **NOTE**: Geo points should be encoded as strings of "lon,lat".
        """
        return self._add_document(
            doc_id,
            conn=None,
            nosave=nosave,
            score=score,
            payload=payload,
            replace=replace,
            partial=partial,
            language=language,
            no_create=no_create,
            **fields
        )

    def add_document_hash(
        self,
        doc_id,
        score=1.0,
        language=None,
        replace=False,
    ):
        """
        Add a hash document to the index.

        Parameters:

        doc_id :
            The document's id. This has to be an existing HASH key in Redis that will hold the fields the index needs.
        score : float
            The document ranking, between 0.0 and 1.0.
        replace : bool
            If True, and the document already is in the index, we perform an update and reindex the document.
        language : str
            Specify the language used for document tokenization.
        """
        return self._add_document_hash(
            doc_id,
            conn=None,
            score=score,
            language=language,
            replace=replace,
        )

    def delete_document(self, doc_id, conn=None, delete_actual_document=False):
        """
        Delete a document from index.
        Returns 1 if the document was deleted, 0 if not.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftdel>`_.

        Parameters:

        delete_actual_document : bool
            If set to True, RediSearch also delete the actual document if it is in the index.
        """
        args = [DEL_CMD, self.index_name, doc_id]
        if conn is None:
            conn = self.client
        if delete_actual_document:
            args.append("DD")

        return conn.execute_command(*args)

    def load_document(self, id):
        """
        Load a single document by id.
        """
        fields = self.client.hgetall(id)
        if six.PY3:
            f2 = {to_string(k): to_string(v) for k, v in fields.items()}
            fields = f2

        try:
            del fields["id"]
        except KeyError:
            pass

        return Document(id=id, **fields)

    def get_documents(self, *ids):
        """
        Returns the full contents of multiple documents.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftmget>`_.

        Parameters:

        ids : list
            The ids of the saved documents.
        """

        return self.client.execute_command(MGET_CMD, self.index_name, *ids)

    def info(self):
        """
        Get info an stats about the the current index, including the number of documents, memory consumption, etc.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftinfo>`_.
        """

        res = self.client.execute_command(INFO_CMD, self.index_name)
        it = six.moves.map(to_string, res)
        return dict(six.moves.zip(it, it))

    def _mk_query_args(self, query):
        args = [self.index_name]

        if isinstance(query, six.string_types):
            # convert the query from a text to a query object
            query = Query(query)
        if not isinstance(query, Query):
            raise ValueError("Bad query type %s" % type(query))

        args += query.get_args()
        return args, query

    def search(self, query):
        """
        Search the index for a given query, and return a result of documents.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftsearch>`_.

        Parameters:

        query : str or search.Query
            The search query. Either a text for simple queries with default parameters,
            or a Query object for complex queries.
        """
        args, query = self._mk_query_args(query)
        st = time.time()
        res = self.execute_command(SEARCH_CMD, *args)

        return Result(
            res,
            not query._no_content,
            duration=(time.time() - st) * 1000.0,
            has_payload=query._with_payloads,
            with_scores=query._with_scores,
        )

    def explain(self, query):
        """
        Return the execution plan for a complex query.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftexplain>`_.

        Parameters:

        query : str or search.Query
            The search query. Either a text for simple queries with default parameters,
            or a Query object for complex queries.
        """
        args, query_text = self._mk_query_args(query)
        return self.execute_command(EXPLAIN_CMD, *args)

    def aggregate(self, query):
        """
        Issue an aggregation query.
        An `AggregateResult` object is returned. You can access the rows from its
        `rows` property, which will always yield the rows of the result.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftaggregate>`_.

        Parameters:

        query :
            This can be either an `AggeregateRequest`, or a `Cursor`.
        """
        if isinstance(query, AggregateRequest):
            has_cursor = bool(query._cursor)
            cmd = [AGGREGATE_CMD, self.index_name] + query.build_args()
        elif isinstance(query, Cursor):
            has_cursor = True
            cmd = [CURSOR_CMD, "READ", self.index_name] + query.build_args()
        else:
            raise ValueError("Bad query", query)

        raw = self.execute_command(*cmd)
        if has_cursor:
            if isinstance(query, Cursor):
                query.cid = raw[1]
                cursor = query
            else:
                cursor = Cursor(raw[1])
            raw = raw[0]
        else:
            cursor = None

        if isinstance(query, AggregateRequest) and query._with_schema:
            schema = raw[0]
            rows = raw[2:]
        else:
            schema = None
            rows = raw[1:]

        res = AggregateResult(rows, cursor, schema)
        return res

    def spellcheck(self, query, distance=None, include=None, exclude=None):
        """
        Issue a spellcheck query.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftspellcheck>`_.

        Parameters:

        query :
            The search query.
        distance :
            The maximal Levenshtein distance for spelling suggestions (default: 1, max: 4).
        include :
            Specifies an inclusion custom dictionary.
        exclude :
            Specifies an exclusion custom dictionary.
        """
        cmd = [SPELLCHECK_CMD, self.index_name, query]

        # todo: All these 3 param are not tested
        if distance:
            cmd.extend(["DISTANCE", distance])
        if include:
            cmd.extend(["TERMS", "INCLUDE", include])
        if exclude:
            cmd.extend(["TERMS", "EXCLUDE", exclude])

        raw = self.execute_command(*cmd)

        corrections = {}
        if raw == 0:
            return corrections

        for _correction in raw:
            if isinstance(_correction, six.integer_types) and _correction == 0:
                continue

            if len(_correction) != 3:
                continue
            if not _correction[2]:
                continue
            if not _correction[2][0]:
                continue

            # For spellcheck output
            # 1)  1) "TERM"
            #     2) "{term1}"
            #     3)  1)  1)  "{score1}"
            #             2)  "{suggestion1}"
            #         2)  1)  "{score2}"
            #             2)  "{suggestion2}"
            #
            # Following dictionary will be made
            # corrections = {
            #     '{term1}': [
            #         {'score': '{score1}', 'suggestion': '{suggestion1}'},
            #         {'score': '{score2}', 'suggestion': '{suggestion2}'}
            #     ]
            # }
            corrections[_correction[1]] = [
                {"score": _item[0], "suggestion": _item[1]} for _item in _correction[2]
            ]

        return corrections

    def dict_add(self, name, *terms):
        """
        Adds terms to a dictionary.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftdictadd>`_.

        Parameters:

        name : str
            Dictionary name.
        terms : list
            List of items for adding to the dictionary.
        """
        cmd = [DICT_ADD_CMD, name]
        cmd.extend(terms)
        raw = self.execute_command(*cmd)
        return raw

    def dict_del(self, name, *terms):
        """
        Deletes terms from a dictionary.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftdictdel>`_.

        Parameters:

        name : str
            Dictionary name.
        terms : list
            List of items for removing from the dictionary.
        """
        cmd = [DICT_DEL_CMD, name]
        cmd.extend(terms)
        raw = self.execute_command(*cmd)
        return raw

    def dict_dump(self, name):
        """
        Dumps all terms in the given dictionary.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftdictdump>`_.

        Parameters:

        name : str
            Dictionary name.
        """
        cmd = [DICT_DUMP_CMD, name]
        raw = self.execute_command(*cmd)
        return raw

    def config_set(self, option, value):
        """
        Set runtime configuration option.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftconfig>`_.

        Parameters:

        option : str
            The name of the configuration option.
        value : str
            A value for the configuration option.
        """
        cmd = [CONFIG_CMD, "SET", option, value]
        raw = self.execute_command(*cmd)
        return raw == "OK"

    def config_get(self, option):
        """
        Get runtime configuration option value.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftconfig>`_.

        Parameters:

        option : str
            The name of the configuration option.
        """
        cmd = [CONFIG_CMD, "GET", option]
        res = {}
        raw = self.execute_command(*cmd)
        if raw:
            for kvs in raw:
                res[kvs[0]] = kvs[1]
        return res

    def tagvals(self, tagfield):
        """
        Return a list of all possible tag values.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#fttagvals>`_.

        Parameters:

        tagfield : str
            Tag field name.
        """
        cmd = self.execute_command(TAGVALS_CMD, self.index_name, tagfield)
        return cmd

    def aliasadd(self, alias):
        """
        Alias a search index - will fail if alias already exists.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftaliasadd>`_.

        Parameters:

        alias : str
            Name of the alias to create.
        """
        cmd = self.execute_command(ALIAS_ADD_CMD, alias, self.index_name)
        return cmd

    def aliasupdate(self, alias):
        """
        Updates an alias - will fail if alias does not already exist.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftaliasupdate>`_.

        Parameters:

        alias : str
            Name of the alias to create.
        """
        cmd = self.execute_command(ALIAS_UPDATE_CMD, alias, self.index_name)
        return cmd

    def aliasdel(self, alias):
        """
        Removes an alias to a search index.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftaliasdel>`_.

        Parameters:

        alias : str
            Name of the alias to delete
        """
        cmd = self.execute_command(ALIAS_DEL_CMD, alias)
        return cmd
