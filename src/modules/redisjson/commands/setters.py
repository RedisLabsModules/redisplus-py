from ..path import Path
from ..utils import *


def jsondelete(client, name, path=Path.rootPath()):
    """
    Deletes the JSON value stored at key ``name`` under ``path``
    """
    return client.execute_command("JSON.DEL", name, str_path(path))
