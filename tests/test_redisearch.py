import pytest
import redis
import bz2
import csv
import time
import os
import sys

from io import TextIOWrapper


from redis import Redis
from redisplus.client import RedisClient
from redisplus.redismod.redisearch import *
from redisplus.redismod.redisearch.client import *
import redisplus.redismod.redisearch.aggregation as aggregations
import redisplus.redismod.redisearch.reducers as reducers
import redisplus.redismod.redisjson

WILL_PLAY_TEXT = os.path.abspath(os.path.dirname(__file__)) + '/will_play_text.csv.bz2'

TITLES_CSV = os.path.abspath(os.path.dirname(__file__)) + '/titles.csv'

v = 0

def waitForIndex(env, idx, timeout=None):
    delay = 0.1
    while True:
        res = env.execute_command('ft.info', idx)
        try:
            res.index('indexing')
        except:
            break

        if int(res[res.index('indexing') + 1]) == 0:
            break

        time.sleep(delay)
        if timeout is not None:
            timeout -= delay
            if timeout <= 0:
                break


def check_version(env, version):
    global v
    if v == 0:
        v = env.execute_command('MODULE LIST')[0][3]
    if int(v) >= version:
        return True
    return False


def getCleanClient(name):
    """
    Gets a client client attached to an index name which is ready to be
    created
    """
    client = Client(name)
    try:
        client.dropindex(delete_documents=True)
    except:
        pass

    return client


def createIndex(client, num_docs=100, definition=None):
    assert isinstance(client, redisplus.redismod.redisearch.client.Client)
    try:
        client.create_index((TextField('play', weight=5.0),
                             TextField('txt'),
                             NumericField('chapter')), definition=definition)
    except redis.ResponseError:
        client.dropindex(delete_documents=True)
        return createIndex(client, num_docs=num_docs, definition=definition)

    chapters = {}
    bzfp = TextIOWrapper(bz2.BZ2File(WILL_PLAY_TEXT), encoding='utf8')

    r = csv.reader(bzfp, delimiter=';')
    for n, line in enumerate(r):
        # ['62816', 'Merchant of Venice', '9', '3.2.74', 'PORTIA', "I'll begin it,--Ding, dong, bell."]

        play, chapter, character, text = line[1], line[2], line[4], line[5]

        key = '{}:{}'.format(play, chapter).lower()
        d = chapters.setdefault(key, {})
        d['play'] = play
        d['txt'] = d.get('txt', '') + ' ' + text
        d['chapter'] = int(chapter or 0)
        if len(chapters) == num_docs:
            break

    indexer = client.batch_indexer(chunk_size=50)
    assert isinstance(indexer, Client.BatchIndexer)
    assert 50 == indexer.chunk_size

    for key, doc in chapters.items():
        indexer.add_document(key, **doc)
    indexer.commit()

@pytest.fixture
def client():
    rc = RedisClient(modules={'redisearch': {"client": Redis(), "index_name": "test"}})
    rc.redisearch.flushdb()
    return rc.redisearch


@pytest.mark.redisearch
def test_base():
    # try not to break the regular client init
    rc = RedisClient(modules={'redisearch': {'client': Redis(), 'index_name': "name"}})


@pytest.mark.integrations
@pytest.mark.redisearch
def testClient(client):

    num_docs = 500
    r = client
    r.flushdb()
    # createIndex(client.client, num_docs=num_docs)
    # for _ in r.retry_with_rdb_reload():
    waitForIndex(r, 'test')
    # verify info
    info = client.info()
    for k in ['index_name', 'index_options', 'attributes', 'num_docs',
                'max_doc_id', 'num_terms', 'num_records', 'inverted_sz_mb',
                'offset_vectors_sz_mb', 'doc_table_size_mb', 'key_table_size_mb',
                'records_per_doc_avg', 'bytes_per_record_avg', 'offsets_per_term_avg',
                'offset_bits_per_record_avg']:
        assert k in info

    assert client.index_name == info['index_name']
    assert num_docs == int(info['num_docs'])

    res = client.search("henry iv")
    assert isinstance(res, Result)
    assert (225 == res.total)
    assert (10 == len(res.docs))
    assert (res.duration > 0)

    for doc in res.docs:
        assert doc.id
        assert (doc.play == 'Henry IV')
        assert (len(doc.txt) > 0)

    # test no content
    res = client.search(Query('king').no_content())
    assert (194 == res.total)
    assert (10 == len(res.docs))
    for doc in res.docs:
        assert ('txt' not in doc.__dict__)
        assert ('play' not in doc.__dict__)

    # test verbatim vs no verbatim
    total = client.search(Query('kings').no_content()).total
    vtotal = client.search(Query('kings').no_content().verbatim()).total
    assert (total > vtotal)

    # test in fields
    txt_total = client.search(Query('henry').no_content().limit_fields('txt')).total
    play_total = client.search(Query('henry').no_content().limit_fields('play')).total
    both_total = client.search(Query('henry').no_content().limit_fields('play', 'txt')).total
    assert (129 == txt_total)
    assert (494 == play_total)
    assert (494 == both_total)

    # test load_document
    doc = client.load_document('henry vi part 3:62')
    assert (doc is not None)
    assert ('henry vi part 3:62' == doc.id)
    assert (doc.play == 'Henry VI Part 3')
    assert (len(doc.txt) > 0)

    # test in-keys
    ids = [x.id for x in client.search(Query('henry')).docs]
    assert (10 == len(ids))
    subset = ids[:5]
    docs = client.search(Query('henry').limit_ids(*subset))
    assert (len(subset) == docs.total)
    ids = [x.id for x in docs.docs]
    assert (set(ids) == set(subset))

    # self.assertRaises(redis.ResponseError, client.search, Query('henry king').return_fields('play', 'nonexist'))

    # test slop and in order
    assert (193 == client.search(Query('henry king')).total)
    assert (3 == client.search(Query('henry king').slop(0).in_order()).total)
    assert (52 == client.search(Query('king henry').slop(0).in_order()).total)
    assert (53 == client.search(Query('henry king').slop(0)).total)
    assert (167 == client.search(Query('henry king').slop(100)).total)

    # test delete document
    client.add_document('doc-5ghs2', play='Death of a Salesman')
    res = client.search(Query('death of a salesman'))
    assert (1 == res.total)

    assert (1 == client.delete_document('doc-5ghs2'))
    res = client.search(Query('death of a salesman'))
    assert (0 == res.total)
    assert (0 == client.delete_document('doc-5ghs2'))

    client.add_document('doc-5ghs2', play='Death of a Salesman')
    res = client.search(Query('death of a salesman'))
    assert (1 == res.total)
    client.delete_document('doc-5ghs2')

def testAddHash(client):
    conn = client.redis()

    with conn as r:
        if check_version(r, 20000):
            return
        # Creating a client with a given index name
        client = Client('idx', port=conn.port)

        client.redis.flushdb()
        # Creating the index definition and schema
        client.create_index((TextField('title', weight=5.0), TextField('body')))

        client.redis.hset(
            'doc1',
            mapping={
                'title': 'RediSearch',
                'body': 'Redisearch impements a search engine on top of redis'
            })

        client.add_document_hash('doc1')

        # Searching with complext parameters:
        q = Query("search engine").verbatim().no_content().paging(0, 5)
        res = client.search(q)
        assert ('doc1' == res.docs[0].id)
