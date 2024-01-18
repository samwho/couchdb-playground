import json

import click
from couch.cluster import Cluster


@click.group()
def http():
    pass


@http.command()
@click.argument("path")
def get(path: str):
    cluster = Cluster.current()
    resp = cluster.get(path)
    click.echo(json.dumps(resp.json(), indent=2))


@http.command()
@click.argument("path")
@click.argument("body")
def post(path: str, body: str):
    cluster = Cluster.current()
    resp = cluster.post(path, json.loads(body))
    click.echo(json.dumps(resp.json(), indent=2))


@http.command()
@click.argument("path")
def delete(path: str):
    cluster = Cluster.current()
    resp = cluster.delete(path)
    click.echo(json.dumps(resp.json(), indent=2))


@http.command()
@click.argument("path")
def put(path: str):
    cluster = Cluster.current()
    resp = cluster.put(path)
    click.echo(json.dumps(resp.json(), indent=2))
