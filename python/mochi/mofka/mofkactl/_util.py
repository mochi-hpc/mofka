import os
import typer
from typing import Optional, List
from pymargo.core import Engine


class ServiceContext:

    def __init__(self, groupfile="mofka.ssg"):
        self.groupfile = groupfile
        import pyssg
        self.protocol = pyssg.get_group_transport_from_file(self.groupfile)

    def __enter__(self):
        self.engine = Engine(self.protocol)
        import pyssg
        pyssg.init()
        from ..client import Client
        client = Client(self.engine.mid)
        self.service = client.connect(self.groupfile)
        return self.service

    def __exit__(self, type, value, traceback):
        self.service = None
        del self.service
        import pyssg
        pyssg.finalize()
        self.engine.finalize()


def parse_config_from_args(args: List[str], prefix):
    config = {}
    while len(args) != 0:
        arg = args[0]
        args.pop(0)
        if not arg.startswith(prefix):
            continue
        if len(args) == 0:
            print(f"Error: configuration argument {arg} has no value")
            raise typer.Exit(code=-1)
        value = args[0]
        args.pop(0)
        if value in ["True", "true"]:
            value = True
        elif value in ["False", "false"]:
            value = False
        elif value.isdecimal():
            value = int(value)
        elif value.replace(".","", 1).isnumeric():
            value = float(value)
        field_names = arg.split(".")
        field_names.pop(0)
        section = config
        for i in range(len(field_names)):
            if i == len(field_names) - 1:
                section[field_names[i]] = value
            else:
                if field_names[i] not in section:
                    section[field_names[i]] = {}
                section = section[field_names[i]]
    return config
