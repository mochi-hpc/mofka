import pymargo.core


class Client:

    def __init__(self, arg):
        if isinstance(arg, pymargo.core.Engine):
            self._engine = arg
            self._owns_engine = False
        elif isinstance(arg, str):
            self._engine = pymargo.core.Engine(arg, pymargo.client)
            self._owns_engine = True
        else:
            raise TypeError(f'Invalid argument type {type(arg)}')
        import pymofka_client
        self._internal = pymofka_client.Client(self._engine.get_internal_mid())

    def __del__(self):
        if self._owns_engine:
            self._engine.finalize()
            del self._engine

    @staticmethod
    def get_protocol_from_ssg_file(group_file: str) -> str:
        import pyssg
        return pyssg.get_group_transport_from_file(group_file)
