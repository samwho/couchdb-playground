import json

import cli
import click
import requests

if __name__ == "__main__":
    try:
        cli.main()
    except requests.RequestException as e:
        click.echo(f"error: {e}")
        if e.response is not None:
            j = e.response.json()
            click.echo(json.dumps(j, indent=2))
        exit(1)
