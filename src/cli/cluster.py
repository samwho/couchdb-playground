import click
import docker
from couch.cluster import Cluster
from rich.console import Console
from rich.table import Table
from utils import parallel_map


@click.group("cluster")
def clster():
    pass


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
@click.option("--nodes", default=3)
@click.option("--image", default="couchdb:3.2.1")
def init(name: str, nodes: int, image: str):
    Cluster.init(name, num_nodes=nodes, image=image)


@clster.command()
@click.argument("name", default="default")
def destroy(name: str):
    client = docker.from_env()
    console = Console()
    filters = {"label": f"cpg={name}"}

    with console.status("stopping nodes..."):
        parallel_map(lambda c: c.stop(), client.containers.list(filters=filters))  # type: ignore

    with console.status("removing nodes..."):
        client.containers.prune(filters=filters)

    with console.status("deleting volumes..."):
        parallel_map(lambda v: v.remove(), client.volumes.list(filters=filters))  # type: ignore
        client.volumes.prune(filters=filters)

    with console.status("deleting network..."):
        client.networks.prune(filters=filters)


@clster.command("list")
def ls():
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
        if network.attrs["Labels"].get("cpg"):  # type: ignore
            name = network.name.split("-")[1]  # type: ignore
            cluster = Cluster.from_name(name)
            table.add_row(name, str(len(cluster.nodes)))

    console = Console()
    console.print(table)
