import pymofka_client
import pymargo
from pymargo.core import Engine


ClientException = pymofka_client.Exception
TopicHandle = pymofka_client.TopicHandle
Validator = pymofka_client.Validator
PartitionSelector = pymofka_client.PartitionSelector
Serializer = pymofka_client.Serializer


class Client:

    def __init__(self, arg):
        self._engine = None
        if isinstance(arg, pymargo.core.Engine):
            mid = arg.mid
        elif isinstance(arg, str):
            self._engine = pymargo.core.Engine(arg, pymargo.server)
            mid = self._engine.mid
        else:
            mid = arg
        self._internal = pymofka_client.Client(mid)

    def connect(self, group_file: str):
        return self._internal.connect(group_file)

    @property
    def config(self):
        return self._internal.config

    def __del__(self):
        del self._internal
        if self._engine is not None:
            self._engine.finalize()
        del self._engine
