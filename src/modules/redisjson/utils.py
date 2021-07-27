from .path import Path


def str_path(p):
    "Returns the string representation of a path if it is of class Path"
    if isinstance(p, Path):
        return p.strPath
    else:
        return p


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