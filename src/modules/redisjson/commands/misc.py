from ..path import Path
from ..utils import *


def jsontype(client, name, path=Path.rootPath()):
    """
    Gets the type of the JSON value under ``path`` from key ``name``
    """
    return client.execute_command("JSON.TYPE", name, str_path(path))


def jsonobjkeys(client, name, path=Path.rootPath()):
    """
    Returns the key names in the dictionary JSON value under ``path`` at key
    ``name``
    """
    return client.execute_command("JSON.OBJKEYS", name, str_path(path))


def jsonobjlen(client, name, path=Path.rootPath()):
    """
    Returns the length of the dictionary JSON value under ``path`` at key
    ``name``
    """
    return client.execute_command("JSON.OBJLEN", name, str_path(path))

def jsondebug(client, name, path=Path.rootPath()):
    """
    Returns the memory usage in bytes of a value under ``path`` from key ``name``.
    """
    return client.execute_command("JSON.DEBUG", "MEMORY", name, str_path(path))
