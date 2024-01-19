import json

import click
from couch.cluster import Cluster
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table
from utils import bytes_to_human


@click.group()
def doc():
    pass


@doc.command()
@click.argument("db_name")
@click.argument("body")
def create(db_name: str, body: str):
    cluster = Cluster.current()
    db = cluster.db(db_name)
    doc = db.insert(json.loads(body))
    Console().print(Syntax(json.dumps(doc.get(), indent=2), "json"))


@doc.command()
@click.argument("db")
def list(db: str):
    cluster = Cluster.current()
    table = Table(header_style="bold magenta", box=None, show_lines=True)
    table.add_column("id")
    table.add_column("rev")
    table.add_column("size")
    table.add_column("body")

    for doc in cluster.db(db).list():
        raw = json.dumps(doc.get(), indent=2)
        highlighted = Syntax(raw, "json")
        table.add_row(doc.id, doc.rev, bytes_to_human(len(raw)), highlighted)

    console = Console()
    console.print(table)
