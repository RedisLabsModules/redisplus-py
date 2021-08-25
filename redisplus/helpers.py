import copy


def bulk_of_jsons(d):
    """Replace serialized JSON values with objects in a bulk array response (list)."""

    def _f(b):
        for index, item in enumerate(b):
            if item is not None:
                b[index] = d(item)
        return b

    return _f


def nativestr(x):
    """Return the decoded binary string, or a string, depending on type."""
    return x.decode("utf-8", "replace") if isinstance(x, bytes) else x


def delist(x):
    """Given a list of binaries, return the stringified version."""
    return [nativestr(obj) for obj in x]


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


def decodeDicKeys(obj):
    """Decode the keys of the given dictionary with utf-8."""
    newobj = copy.copy(obj)
    for k in obj.keys():
        if isinstance(k, bytes):
            newobj[k.decode("utf-8")] = newobj[k]
            newobj.pop(k)
    return newobj
