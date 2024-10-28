import pymofka_client
import pymargo
from pymargo.core import Engine


ClientException = pymofka_client.Exception
TopicHandle = pymofka_client.TopicHandle
Validator = pymofka_client.Validator
PartitionSelector = pymofka_client.PartitionSelector
Serializer = pymofka_client.Serializer
ThreadPool = pymofka_client.ThreadPool
AdaptiveBatchSize = pymofka_client.AdaptiveBatchSize
Producer = pymofka_client.Producer
Consumer = pymofka_client.Consumer
DataDescriptor = pymofka_client.DataDescriptor
Event = pymofka_client.Event
FutureUint = pymofka_client.FutureUint
FutureEvent = pymofka_client.FutureEvent
Ordering = pymofka_client.Ordering
try:
    import pymofka_kafka
    KafkaDriver = pymofka_kafka.KafkaDriver
except ModuleNotFoundError:

    class KafkaDriver:
        def __init__(self, *args, **kwargs):
            raise NotImplementedError("Mofka was not compiled with Kafka support")


class MofkaDriver(pymofka_client.MofkaDriver):

    def __init__(self, group_file, arg=None):
        self._engine = None
        self._mid = None
        if isinstance(arg, pymargo.core.Engine):
            self._mid = arg.mid
        elif isinstance(arg, str):
            self._engine = pymargo.core.Engine(arg, pymargo.server)
            self._mid = self._engine.mid
        else:
            self._mid = arg
        if self._mid is None:
            super().__init__(group_file)
        else:
            super().__init__(group_file, self._mid)

    def create_topic(self, *args, schema: dict|None = None, **kwargs):
        if schema is not None:
            kwargs["validator"] = Validator.from_metadata("schema", {"schema":schema})
        super().create_topic(*args, **kwargs)


class ServiceHandle(MofkaDriver):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        import warnings
        warnings.warn("The ServiceHandle class is deprecated, please use MofkaDriver",
                      DeprecationWarning, stacklevel=2)


class Client:

    def __init__(self, arg):
        import warnings
        warnings.warn("The Client class is deprecated, please use MofkaDriver",
                      DeprecationWarning, stacklevel=2)
        self._engine = None
        self._mid = None
        if isinstance(arg, pymargo.core.Engine):
            self._mid = arg.mid
        elif isinstance(arg, str):
            self._engine = pymargo.core.Engine(arg, pymargo.server)
            self._mid = self._engine.mid
        else:
            self._mid = arg

    def connect(self, group_file: str):
        return ServiceHandle(group_file, self._mid)

    @property
    def config(self):
        return "{}"

    def __del__(self):
        if self._engine is not None:
            self._engine.finalize()
        del self._engine
