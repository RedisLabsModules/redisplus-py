from ..utils import nativestr


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
