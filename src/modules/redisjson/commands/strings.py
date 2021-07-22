from ..path import Path
from ..utils import *


def jsonstrlen(client, name, path=Path.rootPath()):
    """
    Returns the length of the string JSON value under ``path`` at key
    ``name``
    """
    return client.execute_command("JSON.STRLEN", name, str_path(path))


def jsonstrappend(client, name, string, path=Path.rootPath()):
    """
    Appends to the string JSON value under ``path`` at key ``name`` the
    provided ``string``
    """
    return client.execute_command(
        "JSON.STRAPPEND", name, str_path(path), client.encode(string)
    )
