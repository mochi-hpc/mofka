import typer
from typing_extensions import Annotated
from ..spec import MofkaServiceSpec


def convert_range(ctx: typer.Context, param: typer.CallbackParam, value: str):
    try:
        if "," in value:
            result = [int(x) for x in value.split(",")]
            if len(result) != 2 or result[0] > result[1]:
                raise typer.BadParameter(f"Invalid range for {param.name}: {value}")
        else:
            result = int(value)
    except ValueError:
        raise typer.BadParameter(f"Invalid format for {param.name}: {value} (expected int or int,int)")
    return result


app = typer.Typer()


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def generate(
        ctx: typer.Context,
        address: Annotated[
            str, typer.Option("-a", "--address")],
        num_servers: Annotated[
            int, typer.Option("-s", "--num-servers",
                              help="Number of servers")] = 1,
        num_metadata_db_per_proc: Annotated[
            str, typer.Option("-m", "--num-metadata-db-per-proc",
                              callback=convert_range,
                              help="Number of metadata databases per server")] = "1",
        num_data_storage_per_proc: Annotated[
            str, typer.Option("-d", "--num-data-storage-per-proc",
                              callback=convert_range,
                              help="Number of data storage targets per server")] = "1",
        master_db_path_prefixes: Annotated[
            str, typer.Option("--master-db-path-prefixes",
                              help="Prefixes for the master database paths")] = "/tmp/mofka",
        metadata_db_path_prefixes: Annotated[
            str, typer.Option("--metadata-db-path-prefixes",
                              help="Prefixes for the metadata database paths")] = "/tmp/mofka",
        data_storage_path_prefixes: Annotated[
            str, typer.Option("--data-storage-path-prefixes",
                              help="Prefixes for the data storage paths")] = "/tmp/mofka",
        master_db_needs_persistence: Annotated[
            bool, typer.Option("--master-db-needs-persistence",
                               help="Whether the master database needs persistence")] = False,
        metadata_db_needs_persistence: Annotated[
            bool, typer.Option("--metadata-db-needs-persistence",
                               help="Whether the metadata databases need persistence")] = False,
        data_storage_needs_persistence: Annotated[
            bool, typer.Option("--data-storage-needs-persistence",
                               help="Whether the data storage targets need persistence")] = False,
        num_pools_in_servers: Annotated[
            str, typer.Option("-p", "--num-pools-in-servers",
                              callback=convert_range,
                              help="Number of pools in each server")] = "1",
        num_xstreams_in_servers: Annotated[
            str, typer.Option("-x", "--num-xstreams-in-servers",
                              callback=convert_range,
                              help="Number of ES in each server")] = "1",
        allow_more_pools_than_xstreams: Annotated[
            bool, typer.Option("--allow-more-pools-than-xstreams",
                               help="Allow more pools than ES in a server")] = False
        ):
    master_db_path_prefixes = master_db_path_prefixes.split(",")
    metadata_db_path_prefixes = metadata_db_path_prefixes.split(",")
    data_storage_path_prefixes = data_storage_path_prefixes.split(",")
    space = MofkaServiceSpec.space(
        num_servers=num_servers,
        num_metadata_db_per_proc=num_metadata_db_per_proc,
        num_data_storage_per_proc=num_data_storage_per_proc,
        master_db_path_prefixes=master_db_path_prefixes,
        metadata_db_path_prefixes=metadata_db_path_prefixes,
        data_storage_path_prefixes=data_storage_path_prefixes,
        master_db_needs_persistence=master_db_needs_persistence,
        metadata_db_needs_persistence=metadata_db_needs_persistence,
        data_storage_needs_persistence=data_storage_needs_persistence,
        num_pools_in_servers=num_pools_in_servers,
        num_xstreams_in_servers=num_xstreams_in_servers,
        allow_more_pools_than_xstreams=allow_more_pools_than_xstreams).freeze()
    config = space.sample_configuration()
    spec = MofkaServiceSpec.from_config(config=config, address=address)
    print(spec.to_json(indent=4))
