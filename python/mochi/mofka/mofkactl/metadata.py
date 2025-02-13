import typer
from typing_extensions import Annotated
from enum import Enum
from typing import Optional
from ..client import ClientException


app = typer.Typer()


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def add(
        ctx: typer.Context,
        rank: Annotated[
            int, typer.Option("-r", "--rank",
                              help="Rank of the server in which to add the database")],
        type: Annotated[
            str, typer.Option("-t", "--type",
                              help="Type of database")] = "map",
        groupfile: Annotated[
            str, typer.Option("-g", "--groupfile",
                              help="Flock group file of the service")] = "./mofka.json"
        ):
    from ._util import parse_config_from_args
    database_config = parse_config_from_args(ctx.args.copy(), "--config.")
    database_config.update(parse_config_from_args(ctx.args.copy(), "--c."))
    database_dependencies = parse_config_from_args(ctx.args.copy(), "--dependencies.")
    database_dependencies.update(parse_config_from_args(ctx.args.copy(), "--d."))

    # check dependencies format
    for k, v in database_dependencies.items():
        if isinstance(v, str): continue
        if isinstance(v, list):
            for e in v:
                if isinstance(e, str): continue
                print(f"Error: invalid dependency specifications {database_dependencies}")
                raise typer.Exit(code=-1)
            continue
        print(f"Error: invalid dependency specifications {database_dependencies}")
        raise typer.Exit(code=-1)

    from ._util import ServiceContext
    with ServiceContext(groupfile) as service:
        try:
            # check the rank provided
            if rank >= service.num_servers:
                print(f"Error: invalid rank {rank} (should be lower than {service.num_servers})")
            provider = service.add_metadata_provider(
                server_rank=rank,
                database_type=type,
                database_config=database_config,
                dependencies=database_dependencies)
            print(f"Metadata provider started: {provider}")
        except ClientException as err:
            print(f"Error: {err}")
            del service
            raise typer.Exit(code=-1)
        del service
