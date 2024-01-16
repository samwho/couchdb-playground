import click
from couch.cluster import cluster


@click.group()
def db():
    pass


@db.command("create")
@click.argument("name")
def db_create(name: str):
    db = cluster.nodes[0].create_db(name)
    click.echo(f"created db {db.name}")


@db.command("list")
def db_list():
    for db in cluster.dbs():
        click.echo(db.name)
