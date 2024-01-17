import json

import click
from couch.cluster import cluster


@click.group()
def doc():
    pass


@doc.command("create")
@click.argument("db_name")
@click.argument("body")
def doc_create(db_name: str, body: str):
    node = cluster.default_node
    db = node.db(db_name)
    doc = db.insert(json.loads(body))
    click.echo(json.dumps(doc.get(), indent=2))


@doc.command("list")
@click.argument("db_name")
@click.option("--pretty", default=False)
def doc_list(db_name: str, pretty: bool):
    node = cluster.default_node
    db = node.db(db_name)
    for doc in db.list():
        out = json.dumps(doc.get(), indent=2) if pretty else doc.get()
        click.echo(out)
