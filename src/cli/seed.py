import click
from couch.cluster import Cluster


@click.group()
def seed():
    pass


@seed.command()
@click.option("--num-dbs", default=100)
@click.option("--docs-per-db", default=1)
def create(num_dbs: int, docs_per_db: int):
    cluster = Cluster.current()
    cluster.seed(num_dbs, docs_per_db)
    cluster.wait_for_seed(num_dbs, docs_per_db)


@seed.command()
@click.option("--num-dbs", default=100)
@click.option("--docs-per-db", default=1)
def validate(num_dbs: int, docs_per_db: int):
    cluster = Cluster.current()

    try:
        cluster.validate_seed(num_dbs, docs_per_db)
    except Exception:
        exit(1)


@seed.command()
def destroy():
    cluster = Cluster.current()
    cluster.destroy_seed_data()
