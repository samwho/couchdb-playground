import json

import click
from couch.cluster import Cluster
from rich.console import Console
from rich.syntax import Syntax


@click.group()
def config():
    pass


@config.command()
def list():
    cluster = Cluster.current()

    console = Console()
    console.print(Syntax(json.dumps(cluster.config(), indent=2), "json"))


@config.command()
@click.argument("section")
@click.argument("key")
@click.argument("value")
def set(section: str, key: str, value: str):
    cluster = Cluster.current()
    cluster.set_config(section, key, value)


@config.command()
@click.argument("section")
@click.argument("key", required=False)
def get(section: str, key: str | None):
    cluster = Cluster.current()
    console = Console()
    console.print(
        Syntax(json.dumps(cluster.get_config(section, key), indent=2), "json")
    )
