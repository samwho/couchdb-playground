import logging
import sys

import click
from couch.cluster import set_current_cluster, set_default_node
from couch.log import logger

from .cluster import clster
from .db import db
from .doc import doc
from .http import http
from .test import test
from .node import node
from .seed import seed


@click.group()
@click.option("--node", default=0)
@click.option("--cluster", default="default")
@click.option("-v", "--verbose", default=False, is_flag=True)
def main(verbose: bool, node: int, cluster: str):
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    if "cluster" in sys.argv:
        return

    set_current_cluster(cluster)
    set_default_node(node)


main.add_command(db)
main.add_command(doc)
main.add_command(test)
main.add_command(clster)
main.add_command(http)
main.add_command(node)
main.add_command(seed)
