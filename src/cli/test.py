import json
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import system
from time import sleep

import click
import requests
from couch.db import DB
from tqdm import tqdm
from couch.cluster import Cluster


@click.group()
def test():
    pass


def create_seed_data(num_dbs: int = 2000):
    print("creating seed data...")
    cluster = Cluster.current()
    node = cluster.default_node
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = []
        for i in range(num_dbs):

            def do(i):
                db = node.create_db(f"db-{i}")
                db.insert({})

            futures.append(executor.submit(do, i))

        with tqdm(total=num_dbs) as pbar:
            for future in as_completed(futures):
                future.result()
                pbar.update(1)
    print("done")


def wait_for_sync():
    print("waiting for replication...")
    cluster = Cluster.current()
    for node in cluster.nodes:
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []

            def do(db: DB):
                while True:
                    if db.count() == 1:
                        break

            with tqdm(total=2000) as pbar:
                pbar.set_description(node.private_address)
                for i in range(2000):
                    db = node.db(f"db-{i}")
                    futures.append(executor.submit(do, db))

                for future in as_completed(futures):
                    future.result()
                    pbar.update(1)

    print("done")


def assert_all_dbs_have_one_doc(num_dbs: int = 2000):
    cluster = Cluster.current()
    for node in cluster.nodes:
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []

            def do(db: DB):
                count = db.count()
                if count != 1:
                    raise Exception(
                        f"node {node.private_address}/db-{db} has {count} docs"
                    )

            with tqdm(total=num_dbs) as pbar:
                pbar.set_description(node.private_address)
                for db in range(num_dbs):
                    db = node.db(f"db-{db}")
                    futures.append(executor.submit(do, db))

                for future in as_completed(futures):
                    future.result()
                    pbar.update(1)


@test.command()
def lose_data():
    cluster = Cluster.current()
    node = cluster.default_node
    total_dbs = node.total_dbs()
    if total_dbs == 0:
        click.echo("cluster is empty, setting up cluster")
        cluster.setup()
        create_seed_data(num_dbs=2000)
        wait_for_sync()
    elif total_dbs == 2000 + 2:
        click.echo(
            "cluster is already setup, making sure all dbs have 1 document in them..."
        )
        try:
            assert_all_dbs_have_one_doc(num_dbs=2000)
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
        db.cluster.default_node.create_db(db.name)
    except requests.RequestException as e:
        click.echo(f"error while creating db (this is expected): {e.response}")

    click.echo("polling doc count from all nodes...")
    click.echo()

    while True:
        counts = {}
        for node in cluster.nodes:
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


def random_string(length: int) -> str:
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(length))


@test.command()
def lose_node():
    cluster = Cluster.current()
    db = cluster.create_db(random_string(10), n=3)
    click.echo(f"created db {db}")

    node = cluster.nodes[2]
    cluster.remove_node(node)
    click.echo(f"removed node {node}")

    try:
        doc = db.insert({"foo": "bar"})
        click.echo(f"inserted doc {doc}")

        click.echo(f"reading doc from node {cluster.nodes[0]}")
        click.echo(json.dumps(doc.on_node(cluster.nodes[0]).get(), indent=2))

        click.echo(f"reading doc from node {cluster.nodes[1]}")
        click.echo(json.dumps(doc.on_node(cluster.nodes[1]).get(), indent=2))

        cluster.add_node(node)
        click.echo(f"re-added node {node}")

        click.echo(f"reading doc from node {node}")
        click.echo(json.dumps(doc.on_node(node).get(), indent=2))

        cluster.remove_node(node)

        doc.delete()
        click.echo(f"deleted doc {doc}")

        click.echo(f"reading doc from node {cluster.nodes[0]}")
        try:
            doc.on_node(cluster.nodes[0]).get()
        except requests.RequestException as e:
            click.echo(f"error while reading doc (this is expected): {e.response}")

        click.echo(f"reading doc from node {cluster.nodes[1]}")
        try:
            doc.on_node(cluster.nodes[1]).get()
        except requests.RequestException as e:
            click.echo(f"error while reading doc (this is expected): {e.response}")
    finally:
        cluster.add_node(node)
        db.delete()
        click.echo(f"deleted db {db}")
