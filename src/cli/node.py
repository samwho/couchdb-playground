import click
from couch.cluster import Cluster


@click.group()
def node():
    pass


@node.command()
@click.argument("name", type=int)
def destroy(name: int):
    cluster = Cluster.current()
    cluster.get_node(name).destroy()
