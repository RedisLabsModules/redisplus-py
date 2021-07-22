from ..path import Path
from ..utils import *


def jsonarrappend(client, name, path=Path.rootPath(), *args):
    """
    Appends the objects ``args`` to the array under the ``path` in key
    ``name``
    """
    pieces = [name, str_path(path)]
    for o in args:
        pieces.append(client.encode(o))
    return client.execute_command("JSON.ARRAPPEND", *pieces)


def jsonarrindex(client, name, path, scalar, start=0, stop=-1):
    """
    Returns the index of ``scalar`` in the JSON array under ``path`` at key
    ``name``. The search can be limited using the optional inclusive
    ``start`` and exclusive ``stop`` indices.
    """
    return client.execute_command(
        "JSON.ARRINDEX", name, str_path(path), client.encode(scalar), start, stop
    )


def jsonarrinsert(client, name, path, index, *args):
    """
    Inserts the objects ``args`` to the array at index ``index`` under the
    ``path` in key ``name``
    """
    pieces = [name, str_path(path), index]
    for o in args:
        pieces.append(client.encode(o))
    return client.execute_command("JSON.ARRINSERT", *pieces)


def jsonarrlen(client, name, path=Path.rootPath()):
    """
    Returns the length of the array JSON value under ``path`` at key
    ``name``
    """
    return client.execute_command("JSON.ARRLEN", name, str_path(path))


def jsonarrpop(client, name, path=Path.rootPath(), index=-1):
    """
    Pops the element at ``index`` in the array JSON value under ``path`` at
    key ``name``
    """
    return client.execute_command("JSON.ARRPOP", name, str_path(path), index)


def jsonarrtrim(client, name, path, start, stop):
    """
    Trim the array JSON value under ``path`` at key ``name`` to the
    inclusive range given by ``start`` and ``stop``
    """
    return client.execute_command("JSON.ARRTRIM", name, str_path(path), start, stop)
