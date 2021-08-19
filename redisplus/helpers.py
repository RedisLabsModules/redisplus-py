import random
import string


def bulk_of_jsons(d):
    """Replace serialized JSON values with objects in a bulk array response (list)."""

    def _f(b):
        for index, item in enumerate(b):
            if item is not None:
                b[index] = d(item)
        return b

    return _f


def delist(d):
    """Given a list of binaries, return the stringified version."""
    return [_.decode() for _ in d]


def nativestr(x):
    """Return the decoded binary string, or a string, depending on type."""
    return x if isinstance(x, str) else x.decode("utf-8", "replace")


def spaceHolder(response):
    """Return the response without parsing."""
    return response


def parseToList(response):
    """Parse the response to a list."""
    res = []
    for item in response:
        if item is not None:
            res.append(nativestr(item))
        else:
            res.append(None)
    return res


def random_string(length=10):
    """
    Returns a random N character long string.
    """
    return "".join(
        random.choice(string.ascii_lowercase) for x in range(length)
    )  # nosec


def quote_string(v):
    """
    RedisGraph strings must be quoted,
    quote_string wraps given v with quotes incase
    v is a string.
    """

    if isinstance(v, bytes):
        v = v.decode()
    elif not isinstance(v, str):
        return v
    if len(v) == 0:
        return '""'

    v = v.replace('"', '\\"')

    return '"{}"'.format(v)
