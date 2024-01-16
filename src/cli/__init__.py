import click
from couch.cluster import cluster

from .db import db
from .doc import doc
from .lose_data import lose_data


@click.group()
def main():
    pass


@main.command()
def setup():
    if cluster.is_setup():
        click.echo("cluster is setup")
        return
    cluster.setup()


main.add_command(db)
main.add_command(doc)
main.add_command(lose_data)
