import json

import click
from tqdm import tqdm
from couch.cluster import Cluster
from rich.console import Console
from rich.table import Table


@click.group()
def db():
    pass


@db.command()
@click.argument("name")
@click.option("--q", default=2)
@click.option("--n", default=2)
def create(name: str, q: int, n: int):
    cluster = Cluster.current()
    db = cluster.create_db(name, q=q, n=n)
    click.echo(f"created db {db}")


@db.command()
@click.argument("name")
def get(name: str):
    cluster = Cluster.current()
    click.echo(json.dumps(cluster.db(name).get(), indent=2))


@db.command()
def list():
    cluster = Cluster.current()
    table = Table(header_style="bold magenta", box=None, show_lines=True)
    table.add_column("name")
    table.add_column("docs")
    table.add_column("q")
    table.add_column("n")
    table.add_column("r")
    table.add_column("w")
    for db in cluster.dbs():
        info = db.get()
        table.add_row(
            db.name,
            str(info["doc_count"]),
            str(info["cluster"]["q"]),
            str(info["cluster"]["n"]),
            str(info["cluster"]["r"]),
            str(info["cluster"]["w"]),
        )

    console = Console()
    console.print(table)


@db.command()
@click.argument("name")
def delete(name: str):
    cluster = Cluster.current()
    cluster.db(name).delete()
    click.echo(f"deleted db {name}")


@db.command()
def delete_all():
    cluster = Cluster.current()
    for db in tqdm(cluster.dbs()):
        if db.name.startswith("_"):
            continue
        db.delete()
