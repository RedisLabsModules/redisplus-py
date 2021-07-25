from abc import abstractproperty


class ModuleClient(object):
    def __init__(self):

        for key, value in getattr(self, "MODULE_CALLBACKS", {}).items():
            self.CLIENT.set_response_callback(key, value)

    @abstractproperty
    def commands(self):
        raise NotImplementedError("Commands must be implemented by the client.")

    @property
    def client(self):
        return self.CLIENT
