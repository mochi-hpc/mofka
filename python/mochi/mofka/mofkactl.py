import uuid
import typer
from typing import Optional
from typing_extensions import Annotated
import pymargo.core as pymargo
import mochi.bedrock.client as bedrock
import mochi.bedrock.spec as spec


app = typer.Typer(help="Mofka controller CLI.")


def _get_protocol_from_address(address: str) -> str:
    protocol = address.split(":")[0]
    return protocol


@app.command()
def init(
        address: Annotated[
            str,
            typer.Argument(help="Address of the master process")],
        db_type: Annotated[
            str,
            typer.Option(help="Type of the master database")] = "map",
        db_path: Annotated[
            str,
            typer.Option(help="Path to the database")] = "",
        db_provider_id: Annotated[
            int,
            typer.Option(help="Provider Id of the master database provider")] = 65535,
        db_provider_name: Annotated[
            Optional[str],
            typer.Option(help="Name to give the database provider")] = None,
        db_pool: Annotated[
            str,
            typer.Option(help="Pool that the database provider should use")] = "__primary__"
        ):
    """
    Initialize a Mofka service.
    """
    protocol = _get_protocol_from_address(address)
    engine   = pymargo.Engine(addr=protocol, mode=pymargo.client)
    service  = bedrock.Client(engine).make_service_handle(address)
    # Create a provider name if needed
    if db_provider_name is None:
        uuid.uuid4()
        db_provider_name = "mofka_master_" + str(uuid.uuid4())[:8]
    # Fetch the current configuration of the process
    config = service.spec
    # Check if there is already a Yokan database marked with "mofka:master"
    # and if there isn't already a provider with the same name or ID
    for provider in config.providers:
        if provider.type == "yokan" and "mofka:master" in provider.tags:
            raise RuntimeError("Target process already has master database.")
        if provider.provider_id == db_provider_id:
            raise RuntimeError(f"Target process already has a provider with id {db_provider_id}.")
        if provider.name == db_provider_name:
            raise RuntimeError(f"Target process already has a provider named \"{db_provider_name}\".")
    # Check if the Yokan module is loaded
    if "yokan" not in config.libraries:
        raise RuntimeError("Target process does not have a Yokan module loaded.")
    # Check that the requested pool exists
    if config.margo.argobots.pools.count(db_pool) == 0:
        raise RuntimeError("Target process does not have a pool named \"{db_pool}\".")
    # Create a Yokan provider with the specified database type
    service.add_provider(
        config={
            "name": db_provider_name,
            "provider_id": db_provider_id,
            "type": "yokan",
            "pool": db_pool,
            "tags": ["mofka:master"],
            "config": {
                "database":{
                    "type": db_type,
                    "path": db_path
                }
            }
        }
    )


@app.command(name="ct")
@app.command()
def create_topic(
        name: Annotated[
            str,
            typer.Argument(help="Name of the topic to create")],
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


@app.command(name="ap")
@app.command()
def add_partition(
        name: Annotated[
            str,
            typer.Argument(help="Name of the topic to create")],
        address: Annotated[
            str,
            typer.Argument(help="Address of the process in which to create the partition")]):
    """
    Add a partition to a topic.
    """


if __name__ == "__main__":
    app()
