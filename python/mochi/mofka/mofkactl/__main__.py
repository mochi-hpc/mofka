import typer
from typing_extensions import Annotated
from pymargo.core import Engine


app = typer.Typer(help="Mofka CLI.")


from .topic import app as topic_app
app.add_typer(topic_app, name="topic", help="Manipulate topics in a service")

from .partition import app as partition_app
app.add_typer(partition_app, name="partition", help="Manipulate partitions")

from .metadata import app as metadata_app
app.add_typer(metadata_app, name="metadata", help="Manipulate metadata providers")

from .data import app as data_app
app.add_typer(data_app, name="data", help="Manipulate data providers")

from .config import app as config_app
app.add_typer(config_app, name="config", help="Generate (random) initial Mofka configurations")

from .benchmark import app as benchmark_app
app.add_typer(benchmark_app, name="benchmark", help="Generate (random) Benchmark configurations")


if __name__ == "__main__":
    app()
