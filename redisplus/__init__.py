from .client import RedisPlus
import pkg_resources

try:
    __version__ = pkg_resources.get_distribution("redisplus").version
except:
    __version__ = "99.99.99"  # developing

__all__ = ["RedisPlus"]
