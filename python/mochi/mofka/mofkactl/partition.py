import typer
from typing_extensions import Annotated
from enum import Enum
from typing import Optional
from ..client import Validator, PartitionSelector, Serializer, ClientException


app = typer.Typer()


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def add(
        ctx: typer.Context,
        name: Annotated[
            str, typer.Argument(help="Name of the topic")],
        rank: Annotated[
            int, typer.Option("-r", "--rank",
                              help="Rank of the server in which to add the partition")],
        type: Annotated[
            str, typer.Option("-t", "--type",
                              help="Type of partition manager")] = "default",
        pool: Annotated[
            str, typer.Option("-p", "--pool",
                              help="Pool for the partition manager to use")] = "__primary__",
        metadata: Annotated[
            str, typer.Option("-m", "--metadata",
                              help="Metadata provider (default partition manager only)")] = "",
        data: Annotated[
            str, typer.Option("-d", "--data",
                              help="Data provider (default partition manager only)")] = "",
        groupfile: Annotated[
            str, typer.Option("-g", "--groupfile",
                              help="SSG group file of the service")] = "./mofka.ssg"
        ):
    from ._util import parse_config_from_args
    partition_config = parse_config_from_args(ctx.args.copy(), "--config.")
    partition_config.update(parse_config_from_args(ctx.args.copy(), "--c."))
    partition_dependencies = parse_config_from_args(ctx.args.copy(), "--dependencies.")
    partition_dependencies.update(parse_config_from_args(ctx.args.copy(), "--d."))

    # check dependencies format
    for k, v in partition_dependencies.items():
        if isinstance(v, str): continue
        if isinstance(v, list):
            for e in v:
                if isinstance(e, str): continue
                print(f"Error: invalid dependency specifications {partition_dependencies}")
                raise typer.Exit(code=-1)
            continue
        print(f"Error: invalid dependency specifications {partition_dependencies}")
        raise typer.Exit(code=-1)

    from ._util import ServiceContext
    with ServiceContext(groupfile) as service:
        try:
            # check the rank provided
            if rank >= service.num_servers:
                print(f"Error: invalid rank {rank} (should be lower than {service.num_servers})")
            if type == "memory":
                service.add_memory_partition(
                    topic_name=name,
                    server_rank=rank,
                    pool_name=pool)
            elif type == "default":
                service.add_default_partition(
                    topic_name=name,
                    server_rank=rank,
                    metadata_provider=metadata,
                    data_provider=data,
                    partition_config=partition_config,
                    pool_name=pool)
            else:
                service.add_default_partition(
                    topic_name=name,
                    server_rank=rank,
                    partition_type=type,
                    partition_config=partition_config,
                    dependencies=partition_dependencies,
                    pool_name=pool)
        except ClientException as err:
            print(f"Error: {err}")
            del service
            raise typer.Exit(code=-1)
        del service
