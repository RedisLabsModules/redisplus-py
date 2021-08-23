import redis

from .path import Path, str_path


class CommandMixin:
    """json commands."""

    def jsonarrappend(self, name, path=Path.rootPath(), *args):
        """Append the objects ``args`` to the array under the ``path` in key ``name``."""
        pieces = [name, str_path(path)]
        for o in args:
            pieces.append(self._encode(o))
        return self.execute_command("JSON.ARRAPPEND", *pieces)

    def jsonarrindex(self, name, path, scalar, start=0, stop=-1):
        """
        Return the index of ``scalar`` in the JSON array under ``path`` at key ``name``.

        The search can be limited using the optional inclusive ``start`` and exclusive ``stop`` indices.
        """
        return self.execute_command(
            "JSON.ARRINDEX", name, str_path(path), self._encode(scalar), start, stop
        )

    def jsonarrinsert(self, name, path, index, *args):
        """Insert the objects ``args`` to the array at index ``index`` under the ``path` in key ``name``."""
        pieces = [name, str_path(path), index]
        for o in args:
            pieces.append(self._encode(o))
        return self.execute_command("JSON.ARRINSERT", *pieces)

    def jsonforget(self, name, path=Path.rootPath()):
        """Alias for jsondel (delete the JSON value)."""
        return self.execute_command("JSON.FORGET", name, str_path(path))

    def jsonarrlen(self, name, path=Path.rootPath()):
        """Return the length of the array JSON value under ``path`` at key``name``."""
        return self.execute_command("JSON.ARRLEN", name, str_path(path))

    def jsonarrpop(self, name, path=Path.rootPath(), index=-1):
        """Pop the element at ``index`` in the array JSON value under ``path`` at key ``name``."""
        return self.execute_command("JSON.ARRPOP", name, str_path(path), index)

    def jsonarrtrim(self, name, path, start, stop):
        """Trim the array JSON value under ``path`` at key ``name`` to the inclusive range given by ``start`` and ``stop``."""
        return self.execute_command("JSON.ARRTRIM", name, str_path(path), start, stop)

    def jsontype(self, name, path=Path.rootPath()):
        """Get the type of the JSON value under ``path`` from key ``name``."""
        return self.execute_command("JSON.TYPE", name, str_path(path))

    def jsonresp(self, name, path=Path.rootPath()):
        """Return the JSON value under ``path`` at key ``name``."""
        return self.execute_command("JSON.RESP", name, str_path(path))

    def jsonobjkeys(self, name, path=Path.rootPath()):
        """Return the key names in the dictionary JSON value under ``path`` at key ``name``."""
        return self.execute_command("JSON.OBJKEYS", name, str_path(path))

    def jsonobjlen(self, name, path=Path.rootPath()):
        """Return the length of the dictionary JSON value under ``path`` at key ``name``."""
        return self.execute_command("JSON.OBJLEN", name, str_path(path))

    def jsonnumincrby(self, name, path, number):
        """Increment the numeric (integer or floating point) JSON value under ``path`` at key ``name`` by the provided ``number``."""
        return self.execute_command(
            "JSON.NUMINCRBY", name, str_path(path), self._encode(number)
        )

    def jsonnummultby(self, name, path, number):
        """Multiply the numeric (integer or floating point) JSON value under ``path`` at key ``name`` with the provided ``number``."""
        return self.execute_command(
            "JSON.NUMMULTBY", name, str_path(path), self._encode(number)
        )

    def jsonclear(self, name, path=Path.rootPath()):
        """
        Empty arrays and objects (to have zero slots/keys without deleting the array/object).

        Return the count of cleared paths (ignoring non-array and non-objects paths).
        """
        return self.execute_command("JSON.CLEAR", name, str_path(path))

    def jsondel(self, name, path=Path.rootPath()):
        """Delete the JSON value stored at key ``name`` under ``path``."""
        return self.execute_command("JSON.DEL", name, str_path(path))

    def jsonget(self, name, *args, no_escape=False):
        """
        Get the object stored as a JSON value at key ``name``.

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
            return self.execute_command("JSON.GET", *pieces)
        except TypeError:
            return None

    def jsonmget(self, path, *args):
        """Get the objects stored as a JSON values under ``path`` from keys ``args``."""
        pieces = []
        pieces.extend(args)
        pieces.append(str_path(path))
        return self.execute_command("JSON.MGET", *pieces)

    def _executejsonset(self, name, path, obj, nx, xx):

        pieces = [name, str_path(path), self._encode(obj)]

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
        return self.execute_command("JSON.SET", *pieces)

    def jsonset(self, name, path, obj, nx=False, xx=False):
        """
        Set the JSON value at key ``name`` under the ``path`` to ``obj``.

        ``nx`` if set to True, set ``value`` only if it does not exist
        ``xx`` if set to True, set ``value`` only if it exists
        """

        try:
            return self._executejsonset(name, path, obj, nx, xx)
        except TypeError:
            obj_new = {}
            for k, v in obj.items():
                try:
                    obj_new[k.decode('utf-8')] = v
                except AttributeError:
                    obj_new[k] = v
            return self._executejsonset(name, path, obj_new, nx, xx)

    def jsonstrlen(self, name, path=Path.rootPath()):
        """Return the length of the string JSON value under ``path`` at key ``name``."""
        return self.execute_command("JSON.STRLEN", name, str_path(path))

    def jsontoggle(self, name, path=Path.rootPath()):
        """Toggle boolean value under ``path`` at key ``name``, Returning the new value."""
        return self.execute_command("JSON.TOGGLE", name, str_path(path))

    def jsonstrappend(self, name, string, path=Path.rootPath()):
        """Append to the string JSON value under ``path`` at key ``name`` the provided ``string``."""
        return self.execute_command(
            "JSON.STRAPPEND", name, str_path(path), self._encode(string)
        )

    def jsondebug(client, name, path=Path.rootPath()):
        """Return the memory usage in bytes of a value under ``path`` from key ``name``."""
        return client.execute_command("JSON.DEBUG", "MEMORY", name, str_path(path))
