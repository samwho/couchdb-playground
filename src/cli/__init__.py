from couch.cluster import cluster
import cli.lose_data
import cli.setup
import cli.status
import click


@click.group()
def main():
    pass


@main.command()
def status():
    cli.status.main()


@main.command()
def setup():
    cli.setup.main()


@main.command()
def lose_data():
    cli.lose_data.main()


@main.group()
def db():
    pass


@db.command()
@click.argument("name")
def create(name: str):
    db = cluster.nodes[0].create_db(name)
    click.echo(f"created db {db.name}")
