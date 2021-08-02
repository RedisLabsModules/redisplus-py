from redis._compat import nativestr

def list_to_dict(aList):
    return {nativestr(aList[i][0]):nativestr(aList[i][1])
                for i in range(len(aList))}

def parse_range(response):
    return [tuple((l[0], float(l[1]))) for l in response]

def parse_m_range(response):
    res = []
    for item in response:
        res.append({nativestr(item[0]) : [list_to_dict(item[1]),
                                parse_range(item[2])]})
    return res

def parse_get(response):
    if response == []:
        return None
    return (int(response[0]), float(response[1]))

def parse_m_get(response):
    res = []
    for item in response:
        if item[2] == []:
            res.append({nativestr(item[0]) : [list_to_dict(item[1]), None, None]})
        else:
            res.append({nativestr(item[0]) : [list_to_dict(item[1]),
                                int(item[2][0]), float(item[2][1])]})
    return res

def parseToList(response):
    res = []
    for item in response:
        res.append(nativestr(item))
    return res
