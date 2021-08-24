from functools import wraps, partial

from redis.commands import Commands as RedisCommands
import redis
from ..feature import AbstractFeature

from .commands import *


class AI(CommandMixin, AbstractFeature, object):
    """
    Redis client build specifically for the RedisAI module. It takes all the necessary
    parameters to establish the connection and an optional ``debug`` parameter on
    initialization

    Parameters
    ----------

    debug : bool
        If debug mode is ON, then each command that is sent to the server is
        printed to the terminal
    enable_postprocess : bool
        Flag to enable post processing. If enabled, all the bytestring-ed returns
        are converted to python strings recursively and key value pairs will be converted
        to dictionaries. Also note that, this flag doesn't work with pipeline() function
        since pipeline function could have native redis commands (along with RedisAI
        commands)
    """

    REDISAI_COMMANDS_RESPONSE_CALLBACKS = {}

    def __init__(self, client=None, debug=False, enable_postprocess=True):
        self.client = client
        # if debug:
        #     self.execute_command = enable_debug(super().execute_command)
        self.enable_postprocess = enable_postprocess


# def enable_debug(f):
#     @wraps(f)
#     def wrapper(*args):
#        print(*args)
#        return f(*args)
#     return wrapper


class Pipeline(redis.client.Pipeline):
    def __init__(self, enable_postprocess, *args, **kwargs):
        self.enable_postprocess = enable_postprocess
        self.tensorget_processors = []
        self.tensorset_processors = []
        super().__init__(*args, **kwargs)

    def tensorget(self, key, as_numpy=True, as_numpy_mutable=False, meta_only=False):
        self.tensorget_processors.append(
            partial(
                processor.tensorget,
                as_numpy=as_numpy,
                as_numpy_mutable=as_numpy_mutable,
                meta_only=meta_only,
            )
        )
        args = builder.tensorget(key, as_numpy, meta_only)
        return self.execute_command(*args)

    def tensorset(
        self,
        key: AnyStr,
        tensor: Union[np.ndarray, list, tuple],
        shape: Sequence[int] = None,
        dtype: str = None,
    ) -> str:
        args = builder.tensorset(key, tensor, shape, dtype)
        return self.execute_command(*args)

    def _execute_transaction(self, *args, **kwargs):
        res = super()._execute_transaction(*args, **kwargs)
        for i in range(len(res)):
            # tensorget will have minimum 4 values if meta_only = True
            if isinstance(res[i], list) and len(res[i]) >= 4:
                res[i] = self.tensorget_processors.pop(0)(res[i])
        return res

    def _execute_pipeline(self, *args, **kwargs):
        res = super()._execute_pipeline(*args, **kwargs)
        for i in range(len(res)):
            # tensorget will have minimum 4 values if meta_only = True
            if isinstance(res[i], list) and len(res[i]) >= 4:
                res[i] = self.tensorget_processors.pop(0)(res[i])
        return res
