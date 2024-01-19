import click
from couch.cluster import Cluster
from couch.log import logger
from rich.console import Console
from rich.table import Table
from utils import duration_to_human


@click.group()
def node():
    pass


@node.command()
@click.argument("index", type=int)
def destroy(index: int):
    cluster = Cluster.current()
    node = cluster.get_node(index)
    if not node:
        logger.error(f"node {index} does not exist")
        exit(1)
    node.destroy()
    logger.info(f"destroyed node {index}")


@node.command()
@click.option("--count", default=1)
def create(count: int):
    cluster = Cluster.current()
    for _ in range(count):
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
            str(i), duration_to_human(node.uptime()), node.container.name, node.local_address, ok
        )

    console = Console()
    console.print(table)


@node.command()
@click.argument("index", type=int)
def logs(index: int):
    cluster = Cluster.current()
    node = cluster.get_node(index)
    if not node:
        logger.error(f"node {index} does not exist")
        exit(1)

    for chunk in node.container.logs(stream=True):
        print(chunk.decode("utf-8"), end="")


@node.command()
@click.argument("index", type=int)
def restart(index: int):
    cluster = Cluster.current()
    node = cluster.get_node(index)
    if not node:
        logger.error(f"node {index} does not exist")
        exit(1)
    node.restart()
    logger.info(f"restarted node {index}")
