import click
from couch.cluster import Cluster


@click.group()
def node():
    pass


@node.command()
@click.argument("index", type=int)
def destroy(index: int):
    cluster = Cluster.current()
    cluster.get_node(index).destroy()


@node.command()
def list():
    cluster = Cluster.current()
    for node in cluster.nodes:
        click.echo(node.container.name)
