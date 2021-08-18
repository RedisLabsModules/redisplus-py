from abc import ABC
from redis.commands import Commands


class AbstractFeature(Commands, ABC):
    """AbstractBase for all client features.

    This class ensures that we can contstruct modules that access
    the appropriate internal redis client objects.
    """

    def execute_command(self, *args, **kwargs):
        """Execute redis command."""
        return self.client.execute_command(*args, **kwargs)

    @property
    def client(self):
        """Get the client instance."""
        return self.CLIENT