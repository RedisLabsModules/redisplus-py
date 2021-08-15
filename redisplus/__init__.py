from .client import RedisPlus
import pkg_resources


def version():
    try:
        __version__ = pkg_resources.get_distribution("redisplus").version
    except pkg_resources.DistributionNotFound:
        __version__ = "99.99.99"  # developing
    return __version__


__all__ = ["RedisPlus"]
