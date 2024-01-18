import json

import click
from couch.cluster import Cluster
from rich.console import Console
from rich.table import Table


def to_human(bytes: int) -> str:
    if bytes < 1024:
        return f"{bytes}B"
    elif bytes < 1024**2:
        return f"{bytes / 1024:.1f}KB"
    elif bytes < 1024**3:
        return f"{bytes / 1024 ** 2:.1f}MB"
    else:
        return f"{bytes / 1024 ** 3:.1f}GB"


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
    click.echo(json.dumps(doc.get(), indent=2))


@doc.command()
@click.argument("db")
@click.option("--body", default=False, is_flag=True)
@click.option("--indent", default=False, is_flag=True)
def list(db: str, body: bool, indent: bool):
    cluster = Cluster.current()
    table = Table(header_style="bold magenta", box=None, show_lines=True)
    table.add_column("id")
    table.add_column("rev")

    if body:
        table.add_column("size")
        table.add_column("body")

    for doc in cluster.db(db).list():
        if body:
            body = json.dumps(doc.get(), indent=2 if indent else None)
            table.add_row(doc.id, doc.rev, to_human(len(body)), body)
        else:
            table.add_row(doc.id, doc.rev)

    console = Console()
    console.print(table)
