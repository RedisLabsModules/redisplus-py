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
