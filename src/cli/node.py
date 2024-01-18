from datetime import timedelta
import click
from couch.cluster import Cluster
from couch.log import logger
from rich.table import Table
from rich.console import Console


def to_human(delta: timedelta) -> str:
    seconds = delta.total_seconds()
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 60 * 60:
        return f"{seconds / 60:.0f}m"
    elif seconds < 60 * 60 * 24:
        return f"{seconds / 60 / 60:.0f}h"
    else:
        return f"{seconds / 60 / 60 / 24:.0f}d"


@click.group()
def node():
    pass


@node.command()
@click.argument("index", type=int)
def destroy(index: int):
    cluster = Cluster.current()
    cluster.get_node(index).destroy()
    logger.info(f"destroyed node {index}")


@node.command()
def create():
    cluster = Cluster.current()
    node = cluster.add_node()
    logger.info(f"created node {node.name}")


@node.command()
def list():
    cluster = Cluster.current()
    table = Table(
        show_header=True,
        header_style="bold magenta",
        box=None,
        show_lines=True,
    )
    table.add_column("index")
    table.add_column("uptime")
    table.add_column("name")
    table.add_column("address")
    table.add_column("ok")
    for i, node in enumerate(cluster.nodes):
        ok = "✅" if node.ok() else "❌"
        table.add_row(
            str(i), to_human(node.uptime()), node.container.name, node.local_address, ok
        )

    console = Console()
    console.print(table)


@node.command()
@click.argument("index", type=int)
def logs(index: int):
    cluster = Cluster.current()
    node = cluster.get_node(index)

    for chunk in node.container.logs(stream=True):
        print(chunk.decode("utf-8"), end="")
