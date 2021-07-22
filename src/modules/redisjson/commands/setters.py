from ..path import Path
from ..utils import *


def jsondel(client, name, path=Path.rootPath()):
    """
    Deletes the JSON value stored at key ``name`` under ``path``
    """
    return client.execute_command("JSON.DEL", name, str_path(path))


def jsonget(client, name, *args, no_escape=False):
    """
    Get the object stored as a JSON value at key ``name``
    ``args`` is zero or more paths, and defaults to root path
    ```no_escape`` is a boolean flag to add no_escape option to get non-ascii characters
    """
    pieces = [name]
    if no_escape:
        pieces.append("noescape")

    if len(args) == 0:
        pieces.append(Path.rootPath())

    else:
        for p in args:
            pieces.append(str_path(p))

    # Handle case where key doesn't exist. The JSONDecoder would raise a
    # TypeError exception since it can't decode None
    try:
        return client.execute_command("JSON.GET", *pieces)
    except TypeError:
        return None


def jsonmget(client, path, *args):
    """
    Gets the objects stored as a JSON values under ``path`` from
    keys ``args``
    """
    pieces = []
    pieces.extend(args)
    pieces.append(str_path(path))
    return client.execute_command("JSON.MGET", *pieces)


def jsonset(client, name, path, obj, nx=False, xx=False):
    """
    Set the JSON value at key ``name`` under the ``path`` to ``obj``
    ``nx`` if set to True, set ``value`` only if it does not exist
    ``xx`` if set to True, set ``value`` only if it exists
    """
    pieces = [name, str_path(path), client.encode(obj)]

    # Handle existential modifiers
    if nx and xx:
        raise Exception(
            "nx and xx are mutually exclusive: use one, the "
            "other or neither - but not both"
        )
    elif nx:
        pieces.append("NX")
    elif xx:
        pieces.append("XX")
    return client.execute_command("JSON.SET", *pieces)
