"""
Welcome to RedisPlus module.
With this module you can enjoy the commands of all redis modules,
including RedisTimeSeries, RedisBloom, RedisJson, RedisAI and RediSearch.
"""


from .client import RedisPlus
import pkg_resources


def version():
    try:
        __version__ = pkg_resources.get_distribution("redisplus").version
    except pkg_resources.DistributionNotFound:
        __version__ = "99.99.99"  # developing
    return __version__


__all__ = ["RedisPlus"]
