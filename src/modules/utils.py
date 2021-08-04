def nativestr(x):
    """Return the decoded binary string, or a string, depending on type"""
    return x if isinstance(x, str) else x.decode('utf-8', 'replace')
