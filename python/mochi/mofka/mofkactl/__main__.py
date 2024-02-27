import typer
from typing_extensions import Annotated
from pymargo.core import Engine


app = typer.Typer(help="Mofka CLI.")


from .topic import app as topic_app
app.add_typer(topic_app, name="topic", help="Manipulate topics in a service")


if __name__ == "__main__":
    app()
