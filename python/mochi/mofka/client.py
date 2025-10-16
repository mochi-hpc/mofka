import pydiaspora_stream_api
from diaspora_stream.api import *
import pymargo
import pymofka_client
from pymargo.core import Engine


class MofkaDriver(pymofka_client.MofkaDriver):

    def __init__(self, **kwargs):
        super().__init__(kwargs)

    def create_topic(self, *args, schema: dict|None = None, **kwargs):
        if schema is not None:
            kwargs["validator"] = Validator.from_metadata(type="schema", schema=schema)
        super().create_topic(*args, **kwargs)
