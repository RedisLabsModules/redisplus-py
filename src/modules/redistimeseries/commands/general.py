from ..client import Client as TSClient

def info(client, key):
    """Gets information of ``key``"""
    return client.redis.execute_command(TSClient.INFO_CMD, key)


def queryindex(client, filters):
    """Get all the keys matching the ``filter`` list."""
    return client.redis.execute_command(TSClient.QUERYINDEX_CMD, *filters)
