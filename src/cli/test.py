import click
from random import shuffle
from couch.cluster import Cluster
from rich.console import Console
from utils import no_retries, parallel_iter_with_progress


@click.group()
def test():
    pass


@test.command()
@click.option("--num-dbs", default=2000)
@click.option("--docs-per-db", default=1)
def lose_data(num_dbs: int, docs_per_db: int):
    cluster = Cluster.current()
    console = Console()

    console.print("🕵️  checking to see if we can re-use existing data...")
    try:
        cluster.validate_seed(num_dbs, docs_per_db)
        console.print("✅ existing data is valid, re-using")
    except Exception:
        console.print("❌ existing data is invalid, re-seeding")
        cluster.destroy_seed_data()
        cluster.seed(num_dbs, docs_per_db)
        cluster.wait_for_seed(num_dbs, docs_per_db)

    while True:
        node = cluster.nodes[-1]

        with console.status(
            f"destroying node:{node.index} ({node.private_address}))..."
        ):
            node.destroy()
        console.print(f"✅ destroyed node:{node.index} ({node.private_address}))")

        with console.status("adding new node..."):
            node = cluster.add_node()
        console.print(f"✅ added new node:{node.index} ({node.private_address}))")

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
            cluster.wait_for_seed(num_dbs, docs_per_db, timeout=10)
        except Exception:
            console.print("❌ failure while syncing")
            console.print_exception(max_frames=3)

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
        shuffle(range(num_dbs)),
        description="spamming create db requests to new node",
        parallelism=num_dbs // 4,
    )

    cluster.wait_for_seed(num_dbs, docs_per_db)

    try:
        cluster.validate_seed(num_dbs, docs_per_db)
        console.print("all nodes in sync, new node added safely")
    except Exception as e:
        console.print(f"detected data loss: {e}")
        exit(1)
