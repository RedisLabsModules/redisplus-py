from ..path import Path
from ..utils import *


def jsonnumincrby(client, name, path, number):
    """
    Increments the numeric (integer or floating point) JSON value under
    ``path`` at key ``name`` by the provided ``number``
    """
    return client.execute_command(
        "JSON.NUMINCRBY", name, str_path(path), client.encode(number)
    )


def jsonnummultby(client, name, path, number):
    """
    Multiplies the numeric (integer or floating point) JSON value under
    ``path`` at key ``name`` with the provided ``number``
    """
    return client.execute_command(
        "JSON.NUMMULTBY", name, str_path(path), client.encode(number)
    )
