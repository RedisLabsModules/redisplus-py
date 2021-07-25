import pytest
from redisplus.client import RedisClient
from modules.redisjson.utils import Path

@pytest.fixture
def setup_client():
    client = RedisClient({'redisjson': {}})
    client.CLIENT.flushdb()
    return client

# @pytest.mark.integrations
# def test_json_setgetdelete(setup_client):
#     rj = setup_client
#     assert rj.jsonset('foo', Path.rootPath(), 'bar')
#     assert rj.jsonget('foo') == b"bar"
#     assert rj.jsonget('baz') is None
#     assert rj.jsondel('foo') == 1
#     assert rj.jsondel('foo') == 1
#     assert rj.exists('foo') is False

# @pytest.mark.integrations
# def test_justaget(setup_client):
#     rj = setup_client
#     # rj.jsonset('foo', Path.rootPath(), 'bar')
#     assert rj.jsonget('foo') == "bar"


@pytest.mark.integrations
def test_json_get_jset(setup_client):
    rj = setup_client
    assert rj.jsonset('foo', Path.rootPath(), 'bar')
    assert 'bar' == rj.jsonget('foo')
    assert None == rj.jsonget('baz')
    assert 1 == rj.jsondel('foo')
    assert rj.CLIENT.exists('foo') == 0

@pytest.mark.integrations
def test_nonascii_setgetdelete(setup_client):
    rj = setup_client
    assert rj.jsonset('notascii', Path.rootPath(), 'hyvää-élève') is True
    assert 'hyvää-élève' == rj.jsonget('notascii')
    assert 'hyvää-élève' ==  rj.jsonget('notascii', no_escape=True)
    assert 1 == rj.jsondel('notascii')
    assert rj.CLIENT.exists('notascii') == 0

#     def testJSONSetExistentialModifiersShouldSucceed(self):
#         "Test JSONSet's NX/XX flags"

#         obj = { 'foo': 'bar' }
#         self.assertTrue(rj.jsonset('obj', Path.rootPath(), obj))

#         # Test that flags prevent updates when conditions are unmet
#         self.assertFalse(rj.jsonset('obj', Path('foo'), 'baz', nx=True))
#         self.assertFalse(rj.jsonset('obj', Path('qaz'), 'baz', xx=True))

#         # Test that flags allow updates when conditions are met
#         self.assertTrue(rj.jsonset('obj', Path('foo'), 'baz', xx=True))
#         self.assertTrue(rj.jsonset('obj', Path('qaz'), 'baz', nx=True))

#         # Test that flags are mutually exlusive
#         with self.assertRaises(Exception) as context:
#             rj.jsonset('obj', Path('foo'), 'baz', nx=True, xx=True)

#     def testMGetShouldSucceed(self):
#         "Test JSONMGet"

#         rj.jsonset('1', Path.rootPath(), 1)
#         rj.jsonset('2', Path.rootPath(), 2)
#         r = rj.jsonmget(Path.rootPath(), '1', '2')
#         e = [1, 2]
#         self.assertListEqual(e, r)

#     def testTypeShouldSucceed(self):
#         "Test JSONType"

#         rj.jsonset('1', Path.rootPath(), 1)
#         self.assertEqual('integer', rj.jsontype('1'))

#     def testNumIncrByShouldSucceed(self):
#         "Test JSONNumIncrBy"

#         rj.jsonset('num', Path.rootPath(), 1)
#         self.assertEqual(2, rj.jsonnumincrby('num', Path.rootPath(), 1))
#         self.assertEqual(2.5, rj.jsonnumincrby('num', Path.rootPath(), 0.5))
#         self.assertEqual(1.25, rj.jsonnumincrby('num', Path.rootPath(), -1.25))

#     def testNumMultByShouldSucceed(self):
#         "Test JSONNumIncrBy"

#         rj.jsonset('num', Path.rootPath(), 1)
#         self.assertEqual(2, rj.jsonnummultby('num', Path.rootPath(), 2))
#         self.assertEqual(5, rj.jsonnummultby('num', Path.rootPath(), 2.5))
#         self.assertEqual(2.5, rj.jsonnummultby('num', Path.rootPath(), 0.5))

#     def testStrAppendShouldSucceed(self):
#         "Test JSONStrAppend"

#         rj.jsonset('str', Path.rootPath(), 'foo')
#         self.assertEqual(6, rj.jsonstrappend('str', 'bar', Path.rootPath()))
#         self.assertEqual('foobar', rj.jsonget('str', Path.rootPath()))

#     def testStrLenShouldSucceed(self):
#         "Test JSONStrLen"

#         rj.jsonset('str', Path.rootPath(), 'foo')
#         self.assertEqual(3, rj.jsonstrlen('str', Path.rootPath()))
#         rj.jsonstrappend('str', 'bar', Path.rootPath())
#         self.assertEqual(6, rj.jsonstrlen('str', Path.rootPath()))

#     def testArrAppendShouldSucceed(self):
#         "Test JSONSArrAppend"

#         rj.jsonset('arr', Path.rootPath(), [1])
#         self.assertEqual(2, rj.jsonarrappend('arr', Path.rootPath(), 2))
#         self.assertEqual(4, rj.jsonarrappend('arr', Path.rootPath(), 3, 4))
#         self.assertEqual(7, rj.jsonarrappend('arr', Path.rootPath(), *[5, 6, 7]))

#     def testArrIndexShouldSucceed(self):
#         "Test JSONSArrIndex"

#         rj.jsonset('arr', Path.rootPath(), [0, 1, 2, 3, 4])
#         self.assertEqual(1, rj.jsonarrindex('arr', Path.rootPath(), 1))
#         self.assertEqual(-1, rj.jsonarrindex('arr', Path.rootPath(), 1, 2))

#     def testArrInsertShouldSucceed(self):
#         "Test JSONSArrInsert"

#         rj.jsonset('arr', Path.rootPath(), [0, 4])
#         self.assertEqual(5, rj.jsonarrinsert('arr',
#                                              Path.rootPath(), 1, *[1, 2, 3, ]))
#         self.assertListEqual([0, 1, 2, 3, 4], rj.jsonget('arr'))

#     def testArrLenShouldSucceed(self):
#         "Test JSONSArrLen"

#         rj.jsonset('arr', Path.rootPath(), [0, 1, 2, 3, 4])
#         self.assertEqual(5, rj.jsonarrlen('arr', Path.rootPath()))

#     def testArrPopShouldSucceed(self):
#         "Test JSONSArrPop"

#         rj.jsonset('arr', Path.rootPath(), [0, 1, 2, 3, 4])
#         self.assertEqual(4, rj.jsonarrpop('arr', Path.rootPath(), 4))
#         self.assertEqual(3, rj.jsonarrpop('arr', Path.rootPath(), -1))
#         self.assertEqual(2, rj.jsonarrpop('arr', Path.rootPath()))
#         self.assertEqual(0, rj.jsonarrpop('arr', Path.rootPath(), 0))
#         self.assertListEqual([1], rj.jsonget('arr'))

#     def testArrTrimShouldSucceed(self):
#         "Test JSONSArrPop"

#         rj.jsonset('arr', Path.rootPath(), [0, 1, 2, 3, 4])
#         self.assertEqual(3, rj.jsonarrtrim('arr', Path.rootPath(), 1, 3))
#         self.assertListEqual([1, 2, 3], rj.jsonget('arr'))

#     def testObjKeysShouldSucceed(self):
#         "Test JSONSObjKeys"

#         obj = {'foo': 'bar', 'baz': 'qaz'}
#         rj.jsonset('obj', Path.rootPath(), obj)
#         keys = rj.jsonobjkeys('obj', Path.rootPath())
#         keys.sort()
#         exp = [k for k in six.iterkeys(obj)]
#         exp.sort()
#         self.assertListEqual(exp, keys)

#     def testObjLenShouldSucceed(self):
#         "Test JSONSObjLen"

#         obj = {'foo': 'bar', 'baz': 'qaz'}
#         rj.jsonset('obj', Path.rootPath(), obj)
#         self.assertEqual(len(obj), rj.jsonobjlen('obj', Path.rootPath()))

#     def testPipelineShouldSucceed(self):
#         "Test pipeline"

#         p = rj.pipeline()
#         p.jsonset('foo', Path.rootPath(), 'bar')
#         p.jsonget('foo')
#         p.jsondel('foo')
#         p.exists('foo')
#         self.assertListEqual([True, 'bar', 1, False], p.execute())

#     def testCustomEncoderDecoderShouldSucceed(self):
#         "Test a custom encoder and decoder"

#         class CustomClass(object):
#             key = ''
#             val = ''

#             def __init__(self, k='', v=''):
#                 self.key = k
#                 self.val = v

#         class TestEncoder(json.JSONEncoder):
#             def default(self, obj):
#                 if isinstance(obj, CustomClass):
#                     return 'CustomClass:{}:{}'.format(obj.key, obj.val)
#                 return json.JSONEncoder.encode(self, obj)

#         class TestDecoder(json.JSONDecoder):
#             def decode(self, obj):
#                 d = json.JSONDecoder.decode(self, obj)
#                 if isinstance(d, six.string_types) and \
#                         d.startswith('CustomClass:'):
#                     s = d.split(':')
#                     return CustomClass(k=s[1], v=s[2])
#                 return d

#         rj = Client(encoder=TestEncoder(), decoder=TestDecoder(),
#                     port=port, decode_responses=True)
#         rj.flushdb()

#         # Check a regular string
#         self.assertTrue(rj.jsonset('foo', Path.rootPath(), 'bar'))
#         self.assertEqual('string', rj.jsontype('foo', Path.rootPath()))
#         self.assertEqual('bar', rj.jsonget('foo', Path.rootPath()))

#         # Check the custom encoder
#         self.assertTrue(rj.jsonset('cus', Path.rootPath(),
#                                    CustomClass('foo', 'bar')))
#         obj = rj.jsonget('cus', Path.rootPath())
#         self.assertIsNotNone(obj)
#         self.assertEqual(CustomClass, obj.__class__)
#         self.assertEqual('foo', obj.key)
#         self.assertEqual('bar', obj.val)

#     def testUsageExampleShouldSucceed(self):
#         "Test the usage example"

#         # Create a new rejson-py client
#         rj = Client(host='localhost', port=port, decode_responses=True)

#         # Set the key `obj` to some object
#         obj = {
#             'answer': 42,
#             'arr': [None, True, 3.14],
#             'truth': {
#                 'coord': 'out there'
#             }
#         }
#         rj.jsonset('obj', Path.rootPath(), obj)

#         # Get something
#         rv = rj.jsonget('obj', Path('.truth.coord'))
#         self.assertEqual(obj['truth']['coord'], rv)

#         # Delete something (or perhaps nothing), append something and pop it
#         value = "something"
#         rj.jsondel('obj', Path('.arr[0]'))
#         rj.jsonarrappend('obj', Path('.arr'), value)
#         rv = rj.jsonarrpop('obj', Path('.arr'))
#         self.assertEqual(value, rv)

#         # Update something else
#         value = 2.17
#         rj.jsonset('obj', Path('.answer'), value)
#         rv = rj.jsonget('obj', Path('.answer'))
#         self.assertEqual(value, rv)

#         # And use just like the regular redis-py client
#         jp = rj.pipeline()
#         jp.set('foo', 'bar')
#         jp.jsonset('baz', Path.rootPath(), 'qaz')
#         jp.execute()
#         rv1 = rj.get('foo')
#         self.assertEqual('bar', rv1)
#         rv2 = rj.jsonget('baz')
#         self.assertEqual('qaz', rv2)