import typer
from typing_extensions import Annotated
from enum import Enum
from typing import Optional
from ..client import Validator, PartitionSelector, Serializer, ClientException


app = typer.Typer()


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def create(
        ctx: typer.Context,
        name: Annotated[
            str, typer.Argument(help="Name of the pool to create")],
        validator: Annotated[
            str, typer.Option("-v", "--validator",
                              help="Validator implementation")] = "default",
        selector: Annotated[
            str, typer.Option("-p", "--partition-selector",
                              help="Partition selector implementation")] = "default",
        serializer: Annotated[
            str, typer.Option("-s", "--serializer",
                              help="Serializer implementation")] = "default",
        groupfile: Annotated[
            str, typer.Option("-g", "--groupfile",
                              help="SSG group file of the service")] = "./mofka.ssg"
        ):
    from ._util import parse_config_from_args
    validator_config = parse_config_from_args(ctx.args.copy(), "--validator.")
    validator_config.update(parse_config_from_args(ctx.args.copy(), "--v."))
    selector_config = parse_config_from_args(ctx.args.copy(), "--partition-selector.")
    selector_config.update(parse_config_from_args(ctx.args.copy(), "--p."))
    serializer_config = parse_config_from_args(ctx.args.copy(), "--serializer.")
    serializer_config.update(parse_config_from_args(ctx.args.copy(), "--s."))

    from ._util import ServiceContext
    with ServiceContext(groupfile) as service:
        try:
            service.create_topic(
                name,
                Validator.from_metadata(
                    type=validator,
                    metadata=validator_config),
                PartitionSelector.from_metadata(
                    type=selector,
                    metadata=selector_config),
                Serializer.from_metadata(
                    type=serializer,
                    metadata=serializer_config))
        except ClientException as err:
            print(f"Error: {err}")
            raise typer.Exit(code=-1)
        del service
