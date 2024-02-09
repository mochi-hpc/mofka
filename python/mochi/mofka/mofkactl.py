import uuid
from enum import Enum
from pathlib import Path
import typer
from typing import Optional
from typing_extensions import Annotated
import json


class SSGBootstrapMethod(str, Enum):
    mpi  = "mpi"
    init = "init"
    pmix = "pmix"


class YokanDatabaseType(str, Enum):
    map          = "map"
    unoreder_map = "unoreder_map"
    berkeleydb   = "berkeleydb"
    gdbm         = "gdbm"
    leveldb      = "leveldb"
    lmdb         = "lmdb"
    rocksdb      = "rocksdb"
    tkrzw        = "tkrzw"
    unqlite      = "unqlite"


app = typer.Typer(help="Mofka controller CLI.")
topic_app = typer.Typer()
app.add_typer(topic_app, name="topic")


@app.command("init")
def init_config(
        output: Annotated[
            Path,
            typer.Option(
                "-o", "--output",
                help="Name of the JSON configuration file to produce")] = "mofka.json",
        db_type: Annotated[
            YokanDatabaseType,
            typer.Option(help="Type of the master database")] = YokanDatabaseType.map,
        db_path: Annotated[
            Path,
            typer.Option(help="Path to the database")] = "",
        db_provider_id: Annotated[
            int,
            typer.Option(help="Provider ID of the master database provider")] = 1,
        db_provider_name: Annotated[
            str,
            typer.Option(help="Name to give the database provider")] = "mofka_master_database",
        ssg_group_file: Annotated[
            Path,
            typer.Option(help="Name of the SSG group file")] = "mofka.ssg",
        ssg_group_name: Annotated[
            str,
            typer.Option(help="Name of the SSG group")] = "mofka",
        ssg_enable_swim: Annotated[
            bool,
            typer.Option("--ssg-enable-swim/--ssg-disable-swim",
                         help="Whether to enable SWIM in SSG")] = False,
        ssg_bootstrap: Annotated[
            SSGBootstrapMethod,
            typer.Option(help="Bootstrap method for SSG")] = SSGBootstrapMethod.mpi,
        yokan_library_path: Annotated[
            Optional[Path],
            typer.Option(help="Path to libyokan-bedrock-module.so")] = "libyokan-bedrock-module.so",
        _print: Annotated[
            bool,
            typer.Option("-p", "--print",
                         help="Print the configuration to STDOUT")] = False
        ):
    """
    Create a JSON configuration for a new Mofka service.
    """
    import mochi.bedrock.spec as spec
    proc = spec.ProcSpec(margo="?")
    primary_pool = proc.margo.argobots.pools[0]
    proc.ssg.append(
        spec.SSGSpec(
            name=ssg_group_name,
            pool=primary_pool,
            group_file=str(ssg_group_file),
            bootstrap=ssg_bootstrap, #+"|join",
            swim=spec.SwimSpec(
                disabled=not ssg_enable_swim
            )
        ))
    proc.libraries["yokan"] = str(yokan_library_path)
    proc.providers.append(
        spec.ProviderSpec(
            name=db_provider_name,
            provider_id=db_provider_id,
            type="yokan",
            tags=["mofka:master"],
            config={
                "database": {
                    "type": db_type,
                    "path": str(db_path),
                    "create_if_missing": True
                }
            },
            pool=primary_pool))
    config = proc.to_dict()
    config["providers"][0]["__if__"] = f"$__ssg__[\"{ssg_group_name}\"].rank == 0"
    config = json.dumps(config, indent=4)
    if _print:
        from rich import print_json
        print_json(config)
    with open(output, "w+") as f:
        f.write(config)


@topic_app.command(name="create")
def topic_create(
        name: Annotated[
            str,
            typer.Argument(help="Name of the topic to create")],
        ssg_group_file: Annotated[
            Path,
            typer.Option(help="Name of the SSG group file of the service")] = "mofka.ssg",
        validator: Annotated[
            Optional[str],
            typer.Option(help="Validator")] = None,
        partition_selector: Annotated[
            Optional[str],
            typer.Option(help="Partition selector")] = None,
        serializer: Annotated[
            Optional[str],
            typer.Option(help="Serializer")] = None
        ):
    """
    Create a topic.
    """
    from .client import Client
    import pyssg
    import pymargo.core
    protocol = pyssg.get_group_transport_from_file(str(ssg_group_file))
    with pymargo.core.Engine(protocol) as engine:
        pyssg.init()
        service_handle = Client(engine).connect(str(ssg_group_file))
        service_handle.create_topic(name)
        del service_handle
        pyssg.finalize()


if __name__ == "__main__":
    app()
