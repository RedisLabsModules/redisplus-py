import pytest
import redis
import bz2
import csv
import time
import os
import sys

from io import TextIOWrapper
# from .conftest import skip_ifmodversion_lt
from redis import Redis
from redisplus.client import RedisPlus
from redisplus.redismod.redisearch import *
from redisplus.redismod.redisjson.path import Path
import redisplus.redismod.redisearch.aggregation as aggregations
import redisplus.redismod.redisearch.reducers as reducers


WILL_PLAY_TEXT = os.path.abspath(os.path.dirname(__file__)) + '/will_play_text.csv.bz2'

TITLES_CSV = os.path.abspath(os.path.dirname(__file__)) + '/titles.csv'


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


def getClient(name):
    """
    Gets a client client attached to an index name which is ready to be
    created
    """
    rc = RedisPlus(modules={'redisearch': {"client": Redis(), "index_name": name}})
    # rc.redisearch.flushdb()
    return rc.redisearch


def createIndex(client, num_docs=100, definition=None):
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
    rc = RedisPlus(modules={'redisearch': {"client": Redis(), "index_name": "idx"}})
    rc.redisearch.flushdb()
    try:
        rc.redisearch.dropindex(delete_documents=True)
    except:
        pass

    return rc.redisearch


@pytest.mark.redisearch
def test_base():
    # try not to break the regular client init
    rc = RedisPlus(modules={'redisearch': {'client': Redis(), 'index_name': 'idx'}})


@pytest.mark.integrations
@pytest.mark.redisearch
def testClient(client):
    num_docs = 500
    createIndex(client, num_docs=num_docs)
    waitForIndex(client, 'idx')
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


"""
seems like this command was removed from redisearch
https://github.com/RediSearch/RediSearch/pull/1372

@pytest.mark.integrations
@pytest.mark.redisearch
def testAddHash(client):

    # with conn as r:
    #     if check_version(r, 20000):
    #         return
    #     # Creating a client with a given index name
    # client = Client(Redis(), 'idx')

    # client.flushdb()
    # Creating the index definition and schema
    client.create_index((TextField('title', weight=5.0), TextField('body')))

    client.hset(
        'doc1',
        mapping={
            'title': 'RediSearch',
            'body': 'Redisearch implements a search engine on top of redis'
        })

    client.add_document_hash('doc1')

    # Searching with complext parameters:
    q = Query("search engine").verbatim().no_content().paging(0, 5)
    res = client.search(q)
    assert ('doc1' == res.docs[0].id)
"""


@pytest.mark.integrations
@pytest.mark.redisearch
# @skip_ifmodversion_lt("2.2.0", "search")
def testPayloads(client):
    client.create_index((TextField('txt'),))

    client.add_document('doc1', payload='foo baz', txt='foo bar')
    client.add_document('doc2', txt='foo bar')

    q = Query("foo bar").with_payloads()
    res = client.search(q)
    assert (2 == res.total)
    assert ('doc1' == res.docs[0].id)
    assert ('doc2' == res.docs[1].id)
    assert ('foo baz' == res.docs[0].payload)
    assert res.docs[1].payload is None


@pytest.mark.integrations
@pytest.mark.redisearch
def testScores(client):
    client.create_index((TextField('txt'),))

    client.add_document('doc1', txt='foo baz')
    client.add_document('doc2', txt='foo bar')

    q = Query("foo ~bar").with_scores()
    res = client.search(q)
    assert (2 == res.total)
    assert ('doc2' == res.docs[0].id)
    assert (3.0 == res.docs[0].score)
    assert ('doc1' == res.docs[1].id)
    # todo: enable once new RS version is tagged
    # self.assertEqual(0.2, res.docs[1].score)


@pytest.mark.integrations
@pytest.mark.redisearch
def testReplace(client):
    client.create_index((TextField('txt'),))

    client.add_document('doc1', txt='foo bar')
    client.add_document('doc2', txt='foo bar')
    waitForIndex(client, 'idx')

    res = client.search("foo bar")
    assert (2 == res.total)
    client.add_document('doc1', replace=True, txt='this is a replaced doc')

    res = client.search("foo bar")
    assert (1 == res.total)
    assert ('doc2' == res.docs[0].id)

    res = client.search("replaced doc")
    assert (1 == res.total)
    assert ('doc1' == res.docs[0].id)


@pytest.mark.integrations
@pytest.mark.redisearch
def testStopwords(client):
    client.create_index((TextField('txt'),), stopwords=['foo', 'bar', 'baz'])
    client.add_document('doc1', txt='foo bar')
    client.add_document('doc2', txt='hello world')
    waitForIndex(client, 'idx')

    q1 = Query("foo bar").no_content()
    q2 = Query("foo bar hello world").no_content()
    res1, res2 = client.search(q1), client.search(q2)
    assert (0 == res1.total)
    assert (1 == res2.total)


@pytest.mark.integrations
@pytest.mark.redisearch
def testFilters(client):
    client.create_index((TextField('txt'), NumericField('num'), GeoField('loc')))
    client.add_document('doc1', txt='foo bar', num=3.141, loc='-0.441,51.458')
    client.add_document('doc2', txt='foo baz', num=2, loc='-0.1,51.2')

    waitForIndex(client, 'idx')
    # Test numerical filter
    q1 = Query("foo").add_filter(NumericFilter('num', 0, 2)).no_content()
    q2 = Query("foo").add_filter(NumericFilter('num', 2, NumericFilter.INF, minExclusive=True)).no_content()
    res1, res2 = client.search(q1), client.search(q2)

    assert (1 == res1.total)
    assert (1 == res2.total)
    assert ('doc2' == res1.docs[0].id)
    assert ('doc1' == res2.docs[0].id)

    # Test geo filter
    q1 = Query("foo").add_filter(GeoFilter('loc', -0.44, 51.45, 10)).no_content()
    q2 = Query("foo").add_filter(GeoFilter('loc', -0.44, 51.45, 100)).no_content()
    res1, res2 = client.search(q1), client.search(q2)

    assert (1 == res1.total)
    assert (2 == res2.total)
    assert ('doc1' == res1.docs[0].id)

    # Sort results, after RDB reload order may change
    res = [res2.docs[0].id, res2.docs[1].id]
    res.sort()
    assert (['doc1', 'doc2'] == res)


@pytest.mark.integrations
@pytest.mark.redisearch
def testPayloadsWithNoContent(client):
    client.create_index((TextField('txt'),))

    client.add_document('doc1', payload='foo baz', txt='foo bar')
    client.add_document('doc2', payload='foo baz2', txt='foo bar')

    q = Query("foo bar").with_payloads().no_content()
    res = client.search(q)
    assert (2 == len(res.docs))


@pytest.mark.integrations
@pytest.mark.redisearch
def testSortby(client):
    client.create_index((TextField('txt'), NumericField('num', sortable=True)))
    client.add_document('doc1', txt='foo bar', num=1)
    client.add_document('doc2', txt='foo baz', num=2)
    client.add_document('doc3', txt='foo qux', num=3)

    # Test sort
    q1 = Query("foo").sort_by('num', asc=True).no_content()
    q2 = Query("foo").sort_by('num', asc=False).no_content()
    res1, res2 = client.search(q1), client.search(q2)

    assert (3 == res1.total)
    assert ('doc1' == res1.docs[0].id)
    assert ('doc2' == res1.docs[1].id)
    assert ('doc3' == res1.docs[2].id)
    assert (3 == res2.total)
    assert ('doc1' == res2.docs[2].id)
    assert ('doc2' == res2.docs[1].id)
    assert ('doc3' == res2.docs[0].id)


@pytest.mark.integrations
@pytest.mark.redisearch
# @skip_ifmodversion_lt("2.0.0", "search")
def testDropIndex(client):
    """
    Ensure the index gets dropped by data remains by default
    """
    for x in range(20):
        for keep_docs in [[True, {}], [False, {b'name': b'haveit'}]]:
            idx = "HaveIt"
            index = getClient(idx)
            index.hset("index:haveit", mapping={b'name': b'haveit'})
            idef = IndexDefinition(prefix=['index:'])
            index.create_index((TextField('name'),), definition=idef)
            waitForIndex(index, idx)
            index.dropindex(delete_documents=keep_docs[0])
            i = index.hgetall("index:haveit")
            assert (i == keep_docs[1])


@pytest.mark.integrations
@pytest.mark.redisearch
def testExample(client):
    # Creating the index definition and schema
    client.create_index((TextField('title', weight=5.0), TextField('body')))

    # Indexing a document
    client.add_document('doc1', title='RediSearch', body='Redisearch impements a search engine on top of redis')

    # Searching with complex parameters:
    q = Query("search engine").verbatim().no_content().paging(0, 5)

    res = client.search(q)
    assert res is not None


@pytest.mark.integrations
@pytest.mark.redisearch
def testAutoComplete(client):
    ac = AutoCompleter('ac', conn=client)
    n = 0
    with open(TITLES_CSV) as f:
        cr = csv.reader(f)

        for row in cr:
            n += 1
            term, score = row[0], float(row[1])
            # print term, score
            assert (n == ac.add_suggestions(Suggestion(term, score=score)))

    assert (n == ac.len())
    ret = ac.get_suggestions('bad', with_scores=True)
    assert (2 == len(ret))
    assert ('badger' == ret[0].string)
    assert isinstance(ret[0].score, float)
    assert (1.0 != ret[0].score)
    assert ('badalte rishtey' == ret[1].string)
    assert isinstance(ret[1].score, float)
    assert (1.0 != ret[1].score)

    ret = ac.get_suggestions('bad', fuzzy=True, num=10)
    assert (10 == len(ret))
    assert (1.0 == ret[0].score)
    strs = {x.string for x in ret}

    for sug in strs:
        assert (1 == ac.delete(sug))
    # make sure a second delete returns 0
    for sug in strs:
        assert (0 == ac.delete(sug))

    # make sure they were actually deleted
    ret2 = ac.get_suggestions('bad', fuzzy=True, num=10)
    for sug in ret2:
        assert sug.string not in strs

    # Test with payload
    ac.add_suggestions(Suggestion('pay1', payload='pl1'))
    ac.add_suggestions(Suggestion('pay2', payload='pl2'))
    ac.add_suggestions(Suggestion('pay3', payload='pl3'))

    sugs = ac.get_suggestions('pay', with_payloads=True, with_scores=True)
    assert (3 == len(sugs))
    for sug in sugs:
        assert sug.payload
        assert sug.payload.startswith('pl')


@pytest.mark.integrations
@pytest.mark.redisearch
def testNoIndex(client):
    client.create_index(
        (TextField('field'),
         TextField('text', no_index=True, sortable=True),
         NumericField('numeric', no_index=True, sortable=True),
         GeoField('geo', no_index=True, sortable=True),
         TagField('tag', no_index=True, sortable=True)))

    client.add_document('doc1', field='aaa', text='1', numeric='1', geo='1,1', tag='1')
    client.add_document('doc2', field='aab', text='2', numeric='2', geo='2,2', tag='2')
    waitForIndex(client, 'idx')

    res = client.search(Query('@text:aa*'))
    assert (0 == res.total)

    res = client.search(Query('@field:aa*'))
    assert (2 == res.total)

    res = client.search(Query('*').sort_by('text', asc=False))
    assert (2 == res.total)
    assert ('doc2' == res.docs[0].id)

    res = client.search(Query('*').sort_by('text', asc=True))
    assert ('doc1' == res.docs[0].id)

    res = client.search(Query('*').sort_by('numeric', asc=True))
    assert ('doc1' == res.docs[0].id)

    res = client.search(Query('*').sort_by('geo', asc=True))
    assert ('doc1' == res.docs[0].id)

    res = client.search(Query('*').sort_by('tag', asc=True))
    assert ('doc1' == res.docs[0].id)

    # Ensure exception is raised for non-indexable, non-sortable fields
    with pytest.raises(Exception):
        TextFiled('name', no_index=True, sortable=False)
    with pytest.raises(Exception):
        NumericField('name', no_index=True, sortable=False)
    with pytest.raises(Exception):
        GeoField('name', no_index=True, sortable=False)
    with pytest.raises(Exception):
        TagField('name', no_index=True, sortable=False)


@pytest.mark.integrations
@pytest.mark.redisearch
def testPartial(client):
    client.create_index((TextField('f1'), TextField('f2'), TextField('f3')))
    client.add_document('doc1', f1='f1_val', f2='f2_val')
    client.add_document('doc2', f1='f1_val', f2='f2_val')
    client.add_document('doc1', f3='f3_val', partial=True)
    client.add_document('doc2', f3='f3_val', replace=True)
    waitForIndex(client, 'idx')

    # Search for f3 value. All documents should have it
    res = client.search('@f3:f3_val')
    assert (2 == res.total)

    # Only the document updated with PARTIAL should still have the f1 and f2 values
    res = client.search('@f3:f3_val @f2:f2_val @f1:f1_val')
    assert (1 == res.total)


@pytest.mark.integrations
@pytest.mark.redisearch
def testNoCreate(client):
    client.create_index((TextField('f1'), TextField('f2'), TextField('f3')))

    client.add_document('doc1', f1='f1_val', f2='f2_val')
    client.add_document('doc2', f1='f1_val', f2='f2_val')
    client.add_document('doc1', f3='f3_val', no_create=True)
    client.add_document('doc2', f3='f3_val', no_create=True, partial=True)
    waitForIndex(client, 'idx')

    # Search for f3 value. All documents should have it
    res = client.search('@f3:f3_val')
    assert (2 == res.total)

    # Only the document updated with PARTIAL should still have the f1 and f2 values
    res = client.search('@f3:f3_val @f2:f2_val @f1:f1_val')
    assert (1 == res.total)

    with pytest.raises(redis.ResponseError):
        client.add_document('doc3', f2='f2_val', f3='f3_val', no_create=True)


@pytest.mark.integrations
@pytest.mark.redisearch
def testExplain(client):
    client.create_index((TextField('f1'), TextField('f2'), TextField('f3')))
    res = client.explain('@f3:f3_val @f2:f2_val @f1:f1_val')
    assert res


@pytest.mark.integrations
@pytest.mark.redisearch
def testSummarize(client):
    createIndex(client)
    waitForIndex(client, 'idx')

    q = Query('king henry').paging(0, 1)
    q.highlight(fields=('play', 'txt'), tags=('<b>', '</b>'))
    q.summarize('txt')

    doc = sorted(client.search(q).docs)[0]
    assert ('<b>Henry</b> IV' == doc.play)
    assert (
            'ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... ' ==
            doc.txt
    )

    q = Query('king henry').paging(0, 1).summarize().highlight()

    doc = sorted(client.search(q).docs)[0]
    assert ('<b>Henry</b> ... ' == doc.play)
    assert (
            'ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... ' ==
            doc.txt
    )


@pytest.mark.integrations
@pytest.mark.redisearch
# @skip_ifmodversion_lt("2.0.0", "search")
def testAlias(client):
    index1 = getClient("testAlias")
    index2 = getClient("testAlias2")

    index1.hset("index1:lonestar", mapping={'name': 'lonestar'})
    index2.hset("index2:yogurt", mapping={'name': 'yogurt'})

    time.sleep(2)

    def1 = IndexDefinition(prefix=['index1:'], score_field='name')
    def2 = IndexDefinition(prefix=['index2:'], score_field='name')

    index1.create_index((TextField('name'),), definition=def1)
    index2.create_index((TextField('name'),), definition=def2)

    res = index1.search('*').docs[0]
    assert ('index1:lonestar' == res.id)

    # create alias and check for results
    index1.aliasadd("spaceballs")
    alias_client = getClient("spaceballs")
    res = alias_client.search('*').docs[0]
    assert ('index1:lonestar' == res.id)

    # We should throw an exception when trying to add an alias that already exists
    with pytest.raises(Exception):
        index2.aliasadd('spaceballs')

    # update alias and ensure new results
    index2.aliasupdate("spaceballs")
    alias_client2 = getClient('spaceballs')
    res = alias_client2.search('*').docs[0]
    assert ('index2:yogurt' == res.id)

    index2.aliasdel("spaceballs")
    with pytest.raises(Exception):
        alias_client2.search('*').docs[0]


@pytest.mark.integrations
@pytest.mark.redisearch
def testAliasBasic(client):
    # Creating a client with one index
    index1 = getClient('testAlias')
    index1.flushdb()

    index1.create_index((TextField('txt'),))
    index1.add_document('doc1', txt='text goes here')

    index2 = getClient('testAlias2')
    index2.create_index((TextField('txt'),))
    index2.add_document('doc2', txt='text goes here')

    # add the actual alias and check
    index1.aliasadd('myalias')
    alias_client = getClient('myalias')
    res = alias_client.search('*').docs[0]
    assert ('doc1' == res.id)

    # We should throw an exception when trying to add an alias that already exists
    with pytest.raises(Exception):
        index2.aliasadd('myalias')

    # update the alias and ensure we get doc2
    index2.aliasupdate('myalias')
    alias_client2 = getClient('myalias')
    res = alias_client2.search('*').docs[0]
    # todo: not working
    # assert ('doc2' == res.id)

    # delete the alias and expect an error if we try to query again
    index2.aliasdel('myalias')
    with pytest.raises(Exception):
        alias_client2.search('*').docs[0]


@pytest.mark.integrations
@pytest.mark.redisearch
def testTags(client):
    client.create_index((TextField('txt'), TagField('tags')))

    tags = 'foo,foo bar,hello;world'
    tags2 = 'soba,ramen'

    client.add_document('doc1', txt='fooz barz', tags=tags)
    client.add_document('doc2', txt='noodles', tags=tags2)
    waitForIndex(client, 'idx')

    q = Query("@tags:{foo}")
    res = client.search(q)
    assert (1 == res.total)

    q = Query("@tags:{foo bar}")
    res = client.search(q)
    assert (1 == res.total)

    q = Query("@tags:{foo\\ bar}")
    res = client.search(q)
    assert (1 == res.total)

    q = Query("@tags:{hello\\;world}")
    res = client.search(q)
    assert (1 == res.total)

    q2 = client.tagvals('tags')
    assert ((tags.split(',') + tags2.split(',')).sort() == q2.sort())


@pytest.mark.integrations
@pytest.mark.redisearch
def testTextFieldSortableNostem(client):
    # Creating the index definition with sortable and no_stem
    client.create_index((TextField('txt', sortable=True, no_stem=True),))

    # Now get the index info to confirm its contents
    response = client.info()
    assert b'SORTABLE' in response['attributes'][0]
    assert b'NOSTEM' in response['attributes'][0]


@pytest.mark.integrations
@pytest.mark.redisearch
def testAlterSchemaAdd(client):
    # Creating the index definition and schema
    client.create_index((TextField('title'),))

    # Using alter to add a field
    client.alter_schema_add((TextField('body'),))

    # Indexing a document
    client.add_document('doc1', title='MyTitle', body='Some content only in the body')

    # Searching with parameter only in the body (the added field)
    q = Query("only in the body")

    # Ensure we find the result searching on the added body field
    res = client.search(q)
    assert (1 == res.total)


@pytest.mark.integrations
@pytest.mark.redisearch
def testSpellCheck(client):
    client.create_index((TextField('f1'), TextField('f2')))

    client.add_document('doc1', f1='some valid content', f2='this is sample text')
    client.add_document('doc2', f1='very important', f2='lorem ipsum')
    waitForIndex(client, 'idx')

    res = client.spellcheck('impornant')
    assert (b'important' == res[b'impornant'][0]['suggestion'])

    res = client.spellcheck('contnt')
    assert (b'content' == res[b'contnt'][0]['suggestion'])


@pytest.mark.integrations
@pytest.mark.redisearch
def testDictOps(client):
    client.create_index((TextField('f1'), TextField('f2')))
    # Add three items
    res = client.dict_add('custom_dict', 'item1', 'item2', 'item3')
    assert (3 == res)

    # Remove one item
    res = client.dict_del('custom_dict', 'item2')
    assert (1 == res)

    # Dump dict and inspect content
    res = client.dict_dump('custom_dict')
    assert ([b'item1', b'item3'] == res)

    # Remove rest of the items before reload
    client.dict_del('custom_dict', *res)


@pytest.mark.integrations
@pytest.mark.redisearch
def testPhoneticMatcher(client):
    client.create_index((TextField('name'),))

    client.add_document('doc1', name='Jon')
    client.add_document('doc2', name='John')

    res = client.search(Query("Jon"))
    assert (1 == len(res.docs))
    assert ('Jon' == res.docs[0].name)

    # Drop and create index with phonetic matcher
    client.flushdb()

    client.create_index((TextField('name', phonetic_matcher='dm:en'),))

    client.add_document('doc1', name='Jon')
    client.add_document('doc2', name='John')

    res = client.search(Query("Jon"))
    assert (2 == len(res.docs))
    assert (['John', 'Jon'] == sorted([d.name for d in res.docs]))


@pytest.mark.integrations
@pytest.mark.redisearch
def testScorer(client):
    client.create_index((TextField('description'),))

    client.add_document('doc1', description='The quick brown fox jumps over the lazy dog')
    client.add_document('doc2', description='Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do.')

    # default scorer is TFIDF
    res = client.search(Query('quick').with_scores())
    assert (1.0 == res.docs[0].score)
    res = client.search(Query('quick').scorer('TFIDF').with_scores())
    assert (1.0 == res.docs[0].score)
    res = client.search(Query('quick').scorer('TFIDF.DOCNORM').with_scores())
    assert (0.1111111111111111 == res.docs[0].score)
    res = client.search(Query('quick').scorer('BM25').with_scores())
    assert (0.17699114465425977 == res.docs[0].score)
    res = client.search(Query('quick').scorer('DISMAX').with_scores())
    assert (2.0 == res.docs[0].score)
    res = client.search(Query('quick').scorer('DOCSCORE').with_scores())
    assert (1.0 == res.docs[0].score)
    res = client.search(Query('quick').scorer('HAMMING').with_scores())
    assert (0.0 == res.docs[0].score)


@pytest.mark.integrations
@pytest.mark.redisearch
def testGet(client):
    client.create_index((TextField('f1'), TextField('f2')))

    assert ([None] == client.get('doc1'))
    assert ([None, None] == client.get('doc2', 'doc1'))

    client.add_document('doc1', f1='some valid content dd1', f2='this is sample text ff1')
    client.add_document('doc2', f1='some valid content dd2', f2='this is sample text ff2')

    assert ([[b'f1', b'some valid content dd2', b'f2', b'this is sample text ff2']] == client.get('doc2'))
    assert ([[b'f1', b'some valid content dd1', b'f2', b'this is sample text ff1'],
             [b'f1', b'some valid content dd2', b'f2', b'this is sample text ff2']] == client.get('doc1', 'doc2'))


@pytest.mark.integrations
@pytest.mark.redisearch
# @skip_ifmodversion_lt("2.2.0", "search")
def testConfig(client):
    # todo: not working
    # assert client.config_set('TIMEOUT', '100')
    with pytest.raises(redis.ResponseError):
        client.config_set('TIMEOUT', "null")
    # todo: not working
    # res = client.config_get('*')
    # assert (b'100' == res[b'TIMEOUT'])
    # res = client.config_get('TIMEOUT')
    # assert (b'100' == res[b'TIMEOUT'])


@pytest.mark.integrations
@pytest.mark.redisearch
def testAggregations(client):
    # Creating the index definition and schema
    client.create_index((NumericField('random_num'), TextField('title'),
                        TextField('body'), TextField('parent')))

    # Indexing a document
    client.add_document(
        'search',
        title='RediSearch',
        body='Redisearch impements a search engine on top of redis',
        parent='redis',
        random_num=10)
    client.add_document(
        'ai',
        title='RedisAI',
        body=
        'RedisAI executes Deep Learning/Machine Learning models and managing their data.',
        parent='redis',
        random_num=3)
    client.add_document(
        'json',
        title='RedisJson',
        body=
        'RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type.',
        parent='redis',
        random_num=8)

    req = aggregations.AggregateRequest('redis').group_by(
        "@parent",
        reducers.count(),
        reducers.count_distinct('@title'),
        reducers.count_distinctish('@title'),
        reducers.sum("@random_num"),
        reducers.min("@random_num"),
        reducers.max("@random_num"),
        reducers.avg("@random_num"),
        reducers.stddev("random_num"),
        reducers.quantile("@random_num", 0.5),
        reducers.tolist("@title"),
        reducers.first_value("@title"),
        reducers.random_sample("@title", 2),
    )

    res = client.aggregate(req)

    res = res.rows[0]
    assert (len(res) == 26)
    assert (b'redis' == res[1])
    assert (b'3' == res[3])
    assert (b'3' == res[5])
    assert (b'3' == res[7])
    assert (b'21' == res[9])
    assert (b'3' == res[11])
    assert (b'10' == res[13])
    assert (b'7' == res[15])
    assert (b'3.60555127546' == res[17])
    assert (b'10' == res[19])
    assert ([b'RediSearch', b'RedisAI', b'RedisJson'] == res[21])
    assert (b'RediSearch' == res[23])
    assert (2 == len(res[25]))


@pytest.mark.integrations
@pytest.mark.redisearch
# @skip_ifmodversion_lt("2.0.0", "search")
def testIndexDefinition(client):
    """
    Create definition and test its args
    """
    with pytest.raises(RuntimeError):
        IndexDefinition(prefix=['hset:', 'henry'], index_type='json')

    definition = IndexDefinition(prefix=['hset:', 'henry'],
    filter='@f1==32', language='English', language_field='play',
    score_field='chapter', score=0.5, payload_field='txt', index_type=IndexType.JSON)

    assert (['ON', 'JSON', 'PREFIX', 2, 'hset:', 'henry',
    'FILTER', '@f1==32', 'LANGUAGE_FIELD', 'play', 'LANGUAGE', 'English',
    'SCORE_FIELD', 'chapter', 'SCORE', 0.5, 'PAYLOAD_FIELD', 'txt'] ==
    definition.args)

    createIndex(client, num_docs=500, definition=definition)


@pytest.mark.integrations
@pytest.mark.redisearch
# @skip_ifmodversion_lt("2.0.0", "search")
def testCreateClientDefinition(client):
    """
    Create definition with no index type provided,
    and use hset to test the client definition (the default is HASH).
    """
    definition = IndexDefinition(prefix=['hset:', 'henry'])
    createIndex(client, num_docs=500, definition=definition)

    info = client.info()
    assert (494 == int(info['num_docs']))

    client.hset('hset:1', 'f1', 'v1')
    info = client.info()
    assert (495 == int(info['num_docs']))


@pytest.mark.integrations
@pytest.mark.redisearch
# @skip_ifmodversion_lt("2.0.0", "search")
def testCreateClientDefinitionHash(client):
    """
    Create definition with IndexType.HASH as index type (ON HASH),
    and use hset to test the client definition.
    """
    definition = IndexDefinition(prefix=['hset:', 'henry'], index_type=IndexType.HASH)
    createIndex(client, num_docs=500, definition=definition)

    info = client.info()
    assert (494 == int(info['num_docs']))

    client.hset('hset:1', 'f1', 'v1')
    info = client.info()
    assert (495 == int(info['num_docs']))


@pytest.mark.integrations
@pytest.mark.redisearch
# @skip_ifmodversion_lt("2.2.0", "search")
def testCreateClientDefinitionJson(client):
    """
    Create definition with IndexType.JSON as index type (ON JSON),
    and use json client to test it.
    """
    definition = IndexDefinition(prefix=['king:'], index_type=IndexType.JSON)
    client.create_index((TextField('$.name'),), definition=definition)

    rj = RedisPlus(modules={'redisjson': {"client": Redis()}}).redisjson
    # rj = rejson.Client(host='localhost', port=conn.port, decode_responses=True)
    rj.jsonset('king:1', Path.rootPath(), {'name': 'henry'})
    rj.jsonset('king:2', Path.rootPath(), {'name': 'james'})

    res = client.search('henry')
    assert (res.docs[0].id == 'king:1')
    assert res.docs[0].payload is None
    assert (res.docs[0].json == '{"name":"henry"}')
    assert (res.total == 1)


@pytest.mark.integrations
@pytest.mark.redisearch
# @skip_ifmodversion_lt("2.2.0", "search")
def testFieldsAsName(client):
    # create index
    SCHEMA = (
        TextField("$.name", sortable=True, as_name='name'),
        NumericField("$.age", as_name='just_a_number'),
    )
    definition = IndexDefinition(index_type=IndexType.JSON)
    json_client = getClient('idxJson')
    json_client.create_index(SCHEMA, definition=definition)

    # insert json data
    rj = RedisPlus(modules={'redisjson': {"client": Redis()}}).redisjson
    res = rj.jsonset('doc:1', Path.rootPath(), {'name': 'Jon', 'age': 25})
    assert res

    total = json_client.search(Query('Jon').return_fields('name', 'just_a_number')).docs
    assert (1 == len(total))
    assert ('doc:1' == total[0].id)
    assert ('Jon' == total[0].name)
    assert ('25' == total[0].just_a_number)


@pytest.mark.integrations
@pytest.mark.redisearch
# @skip_ifmodversion_lt("2.2.0", "search")
def testSearchReturnFields(client):
    # insert json data
    rj = RedisPlus(modules={'redisjson': {"client": Redis()}}).redisjson
    res = rj.jsonset('doc:1', Path.rootPath(),
                     {"t": "riceratops", "t2": "telmatosaurus", "n": 9072, "flt": 97.2})
    assert res

    # create index json
    definition = IndexDefinition(index_type=IndexType.JSON)
    SCHEMA = (
        TextField("$.t"),
        NumericField("$.flt"),
    )
    json_client = getClient('idxJson')
    json_client.create_index(SCHEMA, definition=definition)

    total = json_client.search(Query('*').return_field("$.t", as_field="txt")).docs
    assert (1 == len(total))
    assert ('doc:1' == total[0].id)
    assert ('riceratops' == total[0].txt)

    total = json_client.search(Query('*').return_field("$.t2", as_field="txt")).docs
    assert (1 == len(total))
    assert ('doc:1' == total[0].id)
    assert ('telmatosaurus' == total[0].txt)