import os
import typer
import json
from typing import Optional, List
from pymargo.core import Engine


class ServiceContext:

    def __init__(self, groupfile="mofka.json"):
        self.groupfile = groupfile
        try:
            with open(groupfile) as f:
                content = json.load(f)
                if (not isinstance(content, dict)) or ("members" not in content):
                    print(f"Error: group file's content seems invalid")
                    raise typer.Exit(code=-1)
                members = content["members"]
                if (not isinstance(members, list)) or (len(members) == 0):
                    print(f"Error: group file's content seems invalid")
                    raise typer.Exit(code=-1)
                member = members[0]
                if (not isinstance(member, dict)) or ("address" not in member):
                    print(f"Error: group file's content seems invalid")
                    raise typer.Exit(code=-1)
                address = member["address"]
                self.protocol = address.split(":")[0]
        except FileNotFoundError:
            print(f"Error: could not find file {self.groupfile}")
            raise typer.Exit(code=-1)

    def __enter__(self):
        self.engine = Engine(self.protocol)
        from ..client import Client
        client = Client(self.engine.mid)
        self.service = client.connect(self.groupfile)
        return self.service

    def __exit__(self, type, value, traceback):
        del self.service
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
                if field_names[i] not in section:
                    section[field_names[i]] = value
                elif isinstance(section[field_names[i]], list):
                    section[field_names[i]].append(value)
                else:
                    section[field_names[i]] = [section[field_names[i]], value]
            else:
                if field_names[i] not in section:
                    section[field_names[i]] = {}
                section = section[field_names[i]]
    return config
