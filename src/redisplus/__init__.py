from .client import RedisClient
import pkg_resources

def version():
    try:
        __version__ = pkg_resources.get_distribution("redisplus").version
    except:
        __version__ = "99.99.99"  # developing

__all__ = ["RedisClient"]
