from redis._compat import nativestr


def bool_ok(response):
    return nativestr(response) == 'OK'


def spaceHolder(response):
    return response


def parseToList(response):
    res = []
    for item in response:
        if item is not None:
            res.append(nativestr(item))
        else:
            res.append(None)
    return res