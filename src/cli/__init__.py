import click
from couch.cluster import cluster as couch_cluster

from .cluster import clster
from .db import db
from .doc import doc
from .http import http
from .test import test


@click.group()
@click.option("--node", default="couchdb1")
def main(node: str):
    n = couch_cluster.get_node(node)
    if n is None:
        click.echo(f'couldn\'t find node "{node}"')
        exit(1)
    couch_cluster.default_node = n
    pass


main.add_command(db)
main.add_command(doc)
main.add_command(test)
main.add_command(clster)
main.add_command(http)
