import click
from couch.cluster import Cluster
from rich.console import Console
from tqdm import tqdm
from utils import no_retries, parallel_iter_with_progress, parallel_map_with_progress


@click.group()
def test():
    pass


@test.command()
@click.option("--num-dbs", default=500)
@click.option("--docs-per-db", default=1)
def lose_data(num_dbs: int, docs_per_db: int):
    cluster = Cluster.current()
    console = Console()

    try:
        cluster.validate_seed(num_dbs, docs_per_db)
    except Exception:
        cluster.destroy_seed_data()
        cluster.seed(num_dbs, docs_per_db)
        cluster.wait_for_seed(num_dbs, docs_per_db)

    while True:
        node = cluster.nodes[-1]

        with console.status(f"destroying node {node.private_address}"):
            node.destroy()

        with console.status("adding new node"):
            node = cluster.add_node()

        def do(i):
            try:
                with no_retries():
                    node.db(f"db-{i}").create()
            except Exception:
                pass

        parallel_iter_with_progress(
            do,
            range(num_dbs),
            description="spamming create db requests to new node",
            parallelism=num_dbs,
        )

        try:
            cluster.validate_seed(num_dbs, docs_per_db)
            console.print("no data loss detected, retrying...")
        except Exception as e:
            console.print(f"detected data loss: {e}")
            break


@test.command()
@click.option("--unsafe", default=False, is_flag=True)
@click.option("--num-dbs", default=1000)
@click.option("--docs-per-db", default=1)
def safely_add_node(unsafe: bool, num_dbs: int, docs_per_db: int):
    cluster = Cluster.current()
    console = Console()

    try:
        cluster.validate_seed(num_dbs, docs_per_db)
    except Exception:
        cluster.destroy_seed_data()
        cluster.seed(num_dbs, docs_per_db)
        cluster.wait_for_seed(num_dbs, docs_per_db)

    with console.status(f"adding new node (maintenance_mode={not unsafe})"):
        node = cluster.add_node(maintenance_mode=not unsafe)
        if not unsafe:
            node.set_config("couchdb", "maintenance_mode", "false")

    def do(i):
        try:
            with no_retries():
                node.db(f"db-{i}").create()
        except Exception:
            pass

    parallel_iter_with_progress(
        do,
        range(num_dbs),
        description="spamming create db requests to new node",
        parallelism=num_dbs,
    )

    with console.status(f"waiting for node {node.private_address} to catch up"):
        cluster.wait_for_seed(num_dbs, docs_per_db)

    with console.status("validating all nodes in sync"):
        try:
            cluster.validate_seed(num_dbs, docs_per_db)
            console.print("all nodes in sync, new node added safely")
        except Exception as e:
            console.print(f"detected data loss: {e}")
            exit(1)
