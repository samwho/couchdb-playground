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
    resp = cluster.membership()
    for node in resp["cluster_nodes"]:
        if node in resp["all_nodes"]:
            click.echo(f"✅ {node}")
        else:
            click.echo(f"❌ {node}")


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
