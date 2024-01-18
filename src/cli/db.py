import json

import click
from tqdm import tqdm
from couch.cluster import Cluster


@click.group()
def db():
    pass


@db.command("create")
@click.argument("name")
@click.option("--q", default=2)
@click.option("--n", default=2)
def db_create(name: str, q: int, n: int):
    cluster = Cluster.current()
    db = cluster.create_db(name, q=q, n=n)
    click.echo(f"created db {db}")


@db.command("get")
@click.argument("name")
def db_get(name: str):
    cluster = Cluster.current()
    click.echo(json.dumps(cluster.db(name).get(), indent=2))


@db.command("list")
def db_list():
    cluster = Cluster.current()
    for db in cluster.dbs():
        click.echo(db.name)


@db.command("delete")
@click.argument("name")
def db_delete(name: str):
    cluster = Cluster.current()
    cluster.db(name).delete()
    click.echo(f"deleted db {name}")


@db.command("delete-all")
def db_delete_all():
    cluster = Cluster.current()
    for db in tqdm(cluster.dbs()):
        if db.name.startswith("_"):
            continue
        db.delete()
