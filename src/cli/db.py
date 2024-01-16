import json
import click
from couch.cluster import cluster
from tqdm import tqdm


@click.group()
def db():
    pass


@db.command("create")
@click.argument("name")
@click.option("--q", default=2)
@click.option("--n", default=2)
def db_create(name: str, q: int, n: int):
    db = cluster.default_node.create_db(name, q=q, n=n)
    click.echo(f"created db {db.name}")


@db.command("get")
@click.argument("name")
def db_get(name: str):
    db = cluster.default_node.db(name)
    click.echo(json.dumps(db.get(), indent=2))


@db.command("list")
def db_list():
    for db in cluster.dbs():
        click.echo(db.name)


@db.command("delete")
@click.argument("name")
def db_delete(name: str):
    db = cluster.default_node.db(name)
    db.delete()
    click.echo(f"deleted db {db.name}")


@db.command("delete-all")
def db_delete_all():
    for db in tqdm(cluster.dbs()):
        if db.name.startswith("_"):
            continue
        db.delete()
