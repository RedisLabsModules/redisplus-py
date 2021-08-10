
def bulk_of_jsons(d):
    "Replace serialized JSON values with objects in a bulk array response (list)"

    def _f(b):
        for index, item in enumerate(b):
            if item is not None:
                b[index] = d(item)
        return b

    return _f


def delist(d):
    """Given a list of binaries, return the stringified version"""
    return [_.decode() for _ in d]


def nativestr(x):
    """Return the decoded binary string, or a string, depending on type"""
    return x if isinstance(x, str) else x.decode("utf-8", "replace")
