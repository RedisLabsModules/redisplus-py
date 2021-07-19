from abc import abstractproperty

class ModuleClient(object):
    
    @abstractproperty
    def commands(self):
        raise NotImplementedError("Commands must be implemented by the client.")

    @property
    def client(self):
        return self.CLIENT