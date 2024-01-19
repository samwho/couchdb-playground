import json

import click
from couch.cluster import Cluster
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table
from utils import parallel_iter_with_progress


@click.group()
def db():
    pass


@db.command()
@click.argument("name")
@click.option("--q", default=2)
@click.option("--n", default=2)
def create(name: str, q: int, n: int):
    cluster = Cluster.current()
    db = cluster.db(name).create(q=q, n=n)
    click.echo(f"created db {db}")


@db.command()
@click.argument("name")
def get(name: str):
    cluster = Cluster.current()
    output = json.dumps(cluster.db(name).describe(), indent=2)
    console = Console()
    console.print(Syntax(output, "json"))


@db.command()
def list():
    cluster = Cluster.current()
    console = Console()

    with console.status("fetching dbs..."):
        table = Table(header_style="bold magenta", box=None, show_lines=True)
        table.add_column("name")
        table.add_column("docs")
        table.add_column("q")
        table.add_column("n")
        table.add_column("r")
        table.add_column("w")
        for info in cluster.dbs_info((db.name for db in cluster.dbs())):
            if "error" in info:
                continue
            table.add_row(
                info["key"],
                str(info["info"]["doc_count"]),
                str(info["info"]["cluster"]["q"]),
                str(info["info"]["cluster"]["n"]),
                str(info["info"]["cluster"]["r"]),
                str(info["info"]["cluster"]["w"]),
            )
    console.print(table)


@db.command()
@click.argument("name")
def delete(name: str):
    cluster = Cluster.current()
    cluster.db(name).destroy()
    click.echo(f"deleted db {name}")


@db.command()
def delete_all():
    cluster = Cluster.current()

    def destroy(db):
        if db.name.startswith("_"):
            return
        db.destroy()

    parallel_iter_with_progress(destroy, cluster.dbs())
