from threading import Thread

import click
from couch.cluster import Cluster, get_default_node
from couch.log import logger
from couch.node import Node
from rich.console import Console
from rich.table import Table
from utils import duration_to_human, no_retries


@click.group()
def node():
    pass


@node.command()
@click.argument("index", type=int)
def destroy(index: int):
    cluster = Cluster.current()
    console = Console()
    node = cluster.get_node(index)
    if not node:
        console.print(f"❌ node:{index} does not exist")
        exit(1)
    with console.status(f"destroying node {index} ({node.private_address}))..."):
        node.destroy()
    console.print(f"✅ destroyed node {index} ({node.private_address}))")


@node.command()
@click.option("--count", default=1)
@click.option("--maintenance-mode", "-m", is_flag=True, default=False)
def create(count: int, maintenance_mode: bool):
    console = Console()
    cluster = Cluster.current()
    for _ in range(count):
        with console.status(f"creating node:{len(cluster.nodes)}..."):
            node = cluster.add_node(maintenance_mode=maintenance_mode)
        console.print(f"✅ created node:{node.index} ({node.private_address}))")


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
        ok = "✅"
        with no_retries():
            if not node.ok():
                ok = "❌"
        table.add_row(
            str(i),
            duration_to_human(node.uptime()),
            node.container.name,
            node.local_address,
            ok,
        )

    console = Console()
    console.print(table)


@node.command()
def logs():
    cluster = Cluster.current()
    console = Console()
    colors = [
        "red",
        "green",
        "blue",
        "yellow",
        "magenta",
        "cyan",
        "white",
        "bright_black",
    ]

    nodes = [cluster.default_node]
    if get_default_node() is None:
        nodes = cluster.nodes

    def tail_logs(node: Node):
        for chunk in node.container.logs(stream=True, follow=True, tail=20):
            console.print(
                f"[{colors[node.index]}]\\[node:{node.index}][/{colors[node.index]}][white]{chunk.decode().strip()}[/white]",
                highlight=False,
            )

    threads = []

    for node in nodes:
        thread = Thread(target=tail_logs, args=(node,), daemon=True)
        thread.start()
        threads.append(thread)

    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        return


@node.command()
@click.argument("index", type=int)
def restart(index: int):
    cluster = Cluster.current()
    console = Console()
    node = cluster.get_node(index)
    if not node:
        console.print(f"❌ node:{index} does not exist")
        exit(1)

    with console.status(f"restarting node {index} ({node.private_address}))..."):
        node.restart()
    console.print(f"✅ restarted node {index} ({node.private_address}))")
