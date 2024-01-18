from collections import defaultdict
import click
import docker
from couch.cluster import Cluster
from rich.table import Table
from rich.console import Console


@click.group("cluster")
def clster():
    pass


@clster.command()
def setup():
    cluster = Cluster.current()
    if cluster.is_setup():
        click.echo("cluster is setup")
        return
    cluster.setup()


@clster.command()
def membership():
    cluster = Cluster.current()

    name_to_index: dict[str, int] = {}
    for i, node in enumerate(cluster.nodes):
        name_to_index[node.name] = i

    superset: list[str] = []
    all_nodes: dict[str, list[str]] = {}
    clustered_nodes: dict[str, list[str]] = {}
    for node in cluster.nodes:
        m = node.membership()
        for member in m["all_nodes"]:
            n = member.split("@")[1].split(".")[0]
            if n not in superset:
                superset.append(n)
            if n not in all_nodes:
                all_nodes[n] = []
            all_nodes[n].append(node.name)
        for member in m["cluster_nodes"]:
            n = member.split("@")[1].split(".")[0]
            if n not in superset:
                superset.append(n)
            if n not in clustered_nodes:
                clustered_nodes[n] = []
            clustered_nodes[n].append(node.name)

    superset.sort(key=lambda n: name_to_index[n])

    table = Table(
        header_style="bold magenta", box=None, show_lines=True, title="all_nodes"
    )
    table.add_column("")

    for member in superset:
        table.add_column(str(name_to_index[member]), justify="center")

    for member in superset:
        row = [str(name_to_index[member])]
        for node in cluster.nodes:
            if node.name in all_nodes[member]:
                row.append("✅")
            else:
                row.append("❌")
        table.add_row(*row)

    console = Console()
    console.print(table)

    table = Table(
        header_style="bold magenta", box=None, show_lines=True, title="cluster_nodes"
    )
    table.add_column("")
    for member in superset:
        table.add_column(str(name_to_index[member]), justify="center")

    for member in superset:
        row = [str(name_to_index[member])]
        for node in cluster.nodes:
            if node.name in clustered_nodes[member]:
                row.append("✅")
            else:
                row.append("❌")
        table.add_row(*row)

    click.echo()
    console.print(table)


@clster.command()
@click.argument("name", default="default")
def init(name: str):
    Cluster.init(name)


@clster.command()
@click.argument("name", default="default")
def destroy(name: str):
    client = docker.from_env()
    filters = {"label": f"cpg={name}"}

    for container in client.containers.list(filters=filters):
        container.stop()  # type: ignore
    client.containers.prune(filters=filters)
    for volume in client.volumes.list(filters=filters):
        volume.remove(force=True)  # type: ignore
    client.volumes.prune(filters=filters)
    client.networks.prune(filters=filters)


@clster.command()
def list():
    client = docker.from_env()
    table = Table(
        show_header=True,
        header_style="bold magenta",
        box=None,
        show_lines=True,
    )

    table.add_column("name")
    table.add_column("nodes")

    for network in client.networks.list():
        if network.attrs["Labels"].get("cpg"):
            name = network.name.split("-")[1]
            cluster = Cluster.from_name(name)
            table.add_row(name, str(len(cluster.nodes)))

    console = Console()
    console.print(table)
