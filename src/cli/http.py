import json
from typing import Callable
import requests
from rich.console import Console
from rich.syntax import Syntax

import click
from couch.cluster import Cluster


@click.group()
def http():
    pass


def do_request(f: Callable[[Cluster], requests.Response]):
    cluster = Cluster.current()
    console = Console()
    try:
        with console.status("waiting for response..."):
            resp = f(cluster)
        console.print(Syntax(json.dumps(resp.json(), indent=2), "json"))
    except requests.exceptions.HTTPError as e:
        console.print(Syntax(e.response.text, "json"))
    except requests.exceptions.ConnectionError as e:
        console.print(f"connection error: {e}")
    except Exception as e:
        console.print(f"unknown error: {e}")


@http.command()
@click.argument("path")
def get(path: str):
    do_request(lambda c: c.get(path, max_attempts=1))


@http.command()
@click.argument("path")
@click.argument("body")
def post(path: str, body: str):
    do_request(lambda c: c.post(path, json.loads(body), max_attempts=1))


@http.command()
@click.argument("path")
def delete(path: str):
    do_request(lambda c: c.delete(path, max_attempts=1))


@http.command()
@click.argument("path")
def put(path: str):
    do_request(lambda c: c.put(path, max_attempts=1))
