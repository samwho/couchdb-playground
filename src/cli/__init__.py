import click
from couch.cluster import cluster

from .db import db
from .doc import doc
from .lose_data import lose_data
from .node import node


@click.group()
@click.option("--node", default="couchdb1")
def main(node: str):
    cluster.default_node = cluster.get_node(node)
    pass


@main.command()
def setup():
    if cluster.is_setup():
        click.echo("cluster is setup")
        return
    cluster.setup()


@main.command()
def membership():
    resp = cluster.membership()
    for node in resp["cluster_nodes"]:
        if node in resp["all_nodes"]:
            click.echo(f"✅ {node}")
        else:
            click.echo(f"❌ {node}")


main.add_command(db)
main.add_command(doc)
main.add_command(node)
main.add_command(lose_data)
