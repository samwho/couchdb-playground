
import click
import requests
from couch.cluster import Cluster
from couch.log import logger
from tqdm import tqdm


@click.group()
def test():
    pass


@test.command()
@click.option("--num-dbs", default=100)
def lose_data(num_dbs: int):
    cluster = Cluster.current()
    node = cluster.default_node
    total_dbs = node.total_dbs()
    if total_dbs == 2:
        cluster.seed(num_dbs, 1)
        cluster.wait_for_seed(num_dbs, 1)
    elif total_dbs == num_dbs + 2:
        logger.info(
            "cluster is already setup, making sure all dbs have 1 document in them..."
        )
        try:
            cluster.validate_seed(num_dbs, 1)
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
                    node.db(f"db-{i}").create()
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


