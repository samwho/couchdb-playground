import json

import click
from couch.cluster import cluster


@click.group()
def http():
    pass


@http.command()
@click.argument("path")
def get(path: str):
    resp = cluster.default_node.get(path)
    click.echo(json.dumps(resp.json(), indent=2))


@http.command()
@click.argument("path")
@click.argument("body")
def post(path: str, body: str):
    resp = cluster.default_node.post(path, json.loads(body))
    click.echo(json.dumps(resp.json(), indent=2))


@http.command()
@click.argument("path")
def delete(path: str):
    resp = cluster.default_node.delete(path)
    click.echo(json.dumps(resp.json(), indent=2))


@http.command()
@click.argument("path")
def put(path: str):
    resp = cluster.default_node.put(path)
    click.echo(json.dumps(resp.json(), indent=2))
