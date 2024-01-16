from os import system
from time import sleep

import click
import requests
from cluster import cluster


def main():
    node = cluster.nodes[0]
    total_dbs = node.total_dbs()
    if total_dbs == 0:
        click.echo("cluster is empty, setting up cluster")
        cluster.setup()
        cluster.create_seed_data(num_dbs=2000)
        cluster.wait_for_sync()
    elif total_dbs == 2000 + 2:
        click.echo(
            "cluster is already setup, making sure all dbs have 1 document in them..."
        )
        try:
            cluster.assert_all_dbs_have_one_doc(num_dbs=2000)
        except Exception as e:
            click.echo(f"cluster not valid: {e}")
            exit(1)
    else:
        click.echo("must run against a fresh cluster, found existing dbs, exiting")
        exit(1)

    click.echo("simulating a node failure by destroying couchdb3 and its data...")
    system("docker compose down -v couchdb3")
    system("docker compose up -d couchdb3")
    system("docker compose restart couchdb1")

    click.echo()
    click.echo("couchdb3 is back up, it should have started replicating again")

    click.echo("finding a database that's missing on couchdb3...")

    db = cluster.random_missing_db()
    if db is None:
        click.echo(
            "no missing databases found, unable to replicate problem this time, try again"
        )
        exit(0)

    click.echo(f"found missing database {db}")
    click.echo(f"creating new empty database {db}...")

    try:
        db.node.create_db(db.name)
    except requests.RequestException as e:
        click.echo(f"error while creating db (this is expected): {e.response}")

    click.echo("polling doc count from all nodes...")
    click.echo()

    dbs = [n.db(db.name) for n in cluster.nodes]

    while True:
        counts = {}
        for db in dbs:
            count = db.count()
            counts[node] = count
            click.echo(f"{db} doc count: {count}")
        click.echo()
        if len(set(counts.values())) == 1:
            val = set(counts.values()).pop()
            if val == 0:
                click.echo(
                    "all nodes reached zero doc count, this means the cluster lost data"
                )
                exit(0)
            else:
                click.echo("all nodes reached a doc count of 1, no data loss this time")
                exit(0)
        sleep(2)
