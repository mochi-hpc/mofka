from typing import Optional
import pymargo.core


class ServiceHandle:

    def __init__(self, client, internal):
        self._client = client
        self._internal = internal

    def create_topic(self, name: str,
                     validator: Optional[str] = None,
                     partition_selector: Optional[str] = None,
                     serializer: Optional[str] = None):
        self._internal.create_topic(name)


class Client:

    def __init__(self, engine: pymargo.core.Engine):
        import pymofka_client
        self._internal = pymofka_client.Client(engine.get_internal_mid())

    def connect(self, *args, **kwargs) -> ServiceHandle:
        return ServiceHandle(
            self, self._internal.connect(*args, **kwargs))
