import click
from couch.cluster import cluster


def main():
    if cluster.is_setup():
        click.echo("cluster is setup")
        return
    cluster.setup()
