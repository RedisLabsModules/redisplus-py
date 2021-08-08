import mock

def mockredisclient(stack):
    """Mock a client connection for non-client interactions."""
    stack.enter_context(mock.patch("redis.connection.Connection.connect"))
    stack.enter_context(mock.patch("redis.connection.ConnectionPool.get_connection"))
    stack.enter_context(mock.patch("redis.connection.HiredisParser.can_read"))
    stack.enter_context(mock.patch("redis.exceptions.ConnectionError"))
    return stack

def stockclosure(f):
    with contextlib.ExitStack() as stack:
        stack = mockredisclient(stack)
