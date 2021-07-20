from .client import RedisClient
import pkg_resources

__version__ = pkg_resources.get_distribution("redisplus").version

__all__ = ["RedisClient"]
