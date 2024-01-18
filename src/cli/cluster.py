
import click
import docker
from couch.cluster import Cluster


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
@click.argument("name")
def init(name: str):
    Cluster.init(name)

@clster.command()
@click.argument("name")
def destroy(name: str):
    client = docker.from_env()
    filters = {"label": f"cpg={name}"}

    for container in client.containers.list(filters=filters):
        container.stop()  # type: ignore
    client.containers.prune(filters=filters)
    for volume in client.volumes.list(filters=filters):
        volume.remove(force=True) # type: ignore
    client.volumes.prune(filters=filters)
    client.networks.prune(filters=filters)
