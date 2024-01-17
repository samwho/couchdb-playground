import click
from couch.cluster import cluster


@click.group("cluster")
def clster():
    pass


@clster.command()
def setup():
    if cluster.is_setup():
        click.echo("cluster is setup")
        return
    cluster.setup()


@clster.command()
def membership():
    resp = cluster.membership()
    for node in resp["cluster_nodes"]:
        if node in resp["all_nodes"]:
            click.echo(f"✅ {node}")
        else:
            click.echo(f"❌ {node}")


@clster.command()
@click.argument("name")
def remove_node(name: str):
    node = cluster.get_node(name)
    if node is None:
        click.echo(f'couldn\'t find node "{name}"')
        exit(1)
    cluster.remove_node(node)


@clster.command()
@click.argument("name")
def add_node(name: str):
    node = cluster.get_node(name)
    if node is None:
        click.echo(f'couldn\'t find node "{name}"')
        exit(1)
    cluster.add_node(node)
