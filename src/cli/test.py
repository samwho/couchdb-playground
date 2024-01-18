import json
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
import requests
from couch.db import DB
from couch.log import logger
from tqdm import tqdm
from couch.cluster import Cluster


@click.group()
def test():
    pass


def create_seed_data(num_dbs: int):
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
            pbar.set_description("seeding cluster")
            for future in as_completed(futures):
                future.result()
                pbar.update(1)


def wait_for_sync(num_dbs: int):
    cluster = Cluster.current()
    for node in cluster.nodes:
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []

            def do(db: DB):
                while True:
                    if db.count() == 1:
                        break

            with tqdm(total=num_dbs) as pbar:
                pbar.set_description(node.private_address)
                for i in range(num_dbs):
                    db = node.db(f"db-{i}")
                    futures.append(executor.submit(do, db))

                for future in as_completed(futures):
                    future.result()
                    pbar.update(1)


def assert_all_dbs_have_one_doc(num_dbs: int):
    cluster = Cluster.current()
    for node in cluster.nodes:
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []

            def do(db: DB):
                count = db.count()
                if count != 1:
                    raise Exception(
                        f"node {node.private_address}/{db} has {count} docs"
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
@click.option("--num-dbs", default=100)
def lose_data(num_dbs: int):
    cluster = Cluster.current()
    node = cluster.default_node
    total_dbs = node.total_dbs()
    if total_dbs == 2:
        create_seed_data(num_dbs)
        wait_for_sync(num_dbs)
    elif total_dbs == num_dbs + 2:
        logger.info(
            "cluster is already setup, making sure all dbs have 1 document in them..."
        )
        try:
            assert_all_dbs_have_one_doc(num_dbs)
        except Exception as e:
            logger.error(f"cluster not valid: {e}")
            exit(1)
    else:
        logger.error("must run against a fresh cluster, found existing dbs, exiting")
        exit(1)

    while True:
        node = cluster.nodes[-1]
        logger.info(f"simulating a node failure by destroying {node.private_address}")

        node.destroy()
        node = cluster.add_node()

        logger.info(f"added new node {node.private_address}")

        logger.info("spamming DB creations...")

        with tqdm(total=num_dbs) as pbar:
            pbar.set_description("spamming new node with DB creations")
            for i in range(num_dbs):
                try:
                    node.create_db(f"db-{i}")
                except requests.exceptions.HTTPError:
                    pass
                pbar.update(1)

        empty_dbs = []
        with tqdm(total=num_dbs) as pbar:
            pbar.set_description("searching for data loss")
            for i in range(num_dbs):
                db_name = f"db-{i}"
                counts = []
                for node in cluster.nodes:
                    counts.append(node.db(db_name).count())

                if all(count == 0 for count in counts):
                    empty_dbs.append(db_name)

                pbar.update(1)

        if empty_dbs:
            logger.error("data loss detected, following DBs are empty:")
            logger.error(f"  {", ".join(empty_dbs)}")
            break
        else:
            logger.info("no data loss detected, retrying...")


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
