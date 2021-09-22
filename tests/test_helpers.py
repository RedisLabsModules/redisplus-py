from redisplus import helpers


def test_random_string():
    assert 10 == len(helpers.random_string())
    assert 5 == len(helpers.random_string(length=5))


def test_quote_string():
    assert helpers.quote_string(10) == 10
    assert helpers.quote_string("abc") == '"abc"'
    assert helpers.quote_string("") == '""'
    assert helpers.quote_string('"') == '"\\""'
    assert helpers.quote_string('"') == '"\\""'
    assert helpers.quote_string('a"a') == '"a\\"a"'

def test_stringify_param_value(self):
    cases = [
        [
            "abc", '"abc"'
        ],
        [
            None, "null"
        ],
        [
            ["abc", 123, None],
            '["abc",123,null]'
        ],
        [
            {'age': 2, 'color': 'orange'},
            '{age:2,color:"orange"}'
        ],
        [
            [{'age': 2, 'color': 'orange'}, {'age': 7, 'color': 'gray'}, ],
            '[{age:2,color:"orange"},{age:7,color:"gray"}]'
        ],
    ]
    for param, expected in cases:
        observed = helpers.stringify_param_value(param)
        self.assertEqual(observed, expected)
