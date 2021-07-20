from ..path import Path
from ..utils import *


def jsonstrlen(client, name, path=Path.rootPath()):
    """
    Returns the length of the string JSON value under ``path`` at key
    ``name``
    """
    return client.execute_command("JSON.STRLEN", name, str_path(path))
