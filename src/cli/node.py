import click
from couch.cluster import cluster


@click.group()
def node():
    pass


@node.command("remove")
@click.argument("name")
def node_remove(name: str):
    cluster.remove_node(name)
