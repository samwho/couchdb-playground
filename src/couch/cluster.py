from time import sleep
from typing import Any, Generator, Iterable, cast

import click
import docker
import requests
from couch.log import logger
from docker.models.containers import Container
from utils import parallel_iter_with_progress, parallel_map

from .credentials import password, username
from .db import DB
from .node import Node
from .types import DBInfo, MembershipResponse

_current_cluster = "default"
_default_node = 0


def set_current_cluster(name: str):
    global _current_cluster
    _current_cluster = name


def set_default_node(node: int):
    global _default_node
    _default_node = node


class Cluster:
    name: str
    nodes: list[Node]

    @staticmethod
    def init(name: str, num_nodes: int = 3) -> "Cluster":
        client = docker.from_env()

        if len(client.networks.list(filters={"label": f"cpg={name}"})) != 0:
            click.echo(f'cluster with name "{name}" already exists')
            exit(1)

        network = client.networks.create(
            f"cpg-{name}", driver="bridge", labels={"cpg": name}
        )
        logger.debug(f"created network {network.name}")  # type: ignore

        nodes = list(parallel_map(lambda _: Node.create(name), range(num_nodes)))
        cluster = Cluster(name, nodes)

        logger.debug("waiting for nodes to become healthy...")
        while not cluster.ok():
            sleep(0.5)

        logger.debug("nodes are healthy, setting up cluster...")
        cluster.setup()

        logger.debug("cluster setup complete!")
        return cluster

    @staticmethod
    def from_name(name: str) -> "Cluster":
        client = docker.from_env()

        network = client.networks.list(filters={"label": f"cpg={name}"})
        if len(network) == 0:
            click.echo(f'cluster with name "{name}" does not exist')
            click.echo(f"run `python src/main.py cluster init {name}` to create it")
            exit(1)

        containers = client.containers.list(filters={"label": f"cpg={name}"})
        nodes = [Node(0, cast(Container, container)) for container in containers]
        return Cluster(name, nodes)

    @staticmethod
    def current() -> "Cluster":
        return Cluster.from_name(_current_cluster)

    def __init__(self, name: str, nodes: list[Node]):
        self.name = name
        self.nodes = nodes
        self.reorder_nodes()

    def reorder_nodes(self):
        self.nodes.sort(key=lambda n: n.started_at())
        for i, node in enumerate(self.nodes):
            node.index = i

        for node in self.nodes:
            node.cluster = self
            node.reload()

    @property
    def default_node(self) -> Node:
        return self.nodes[_default_node]

    def setup(self):
        if self.is_setup():
            logger.debug("cluster is already setup, skipping setup")
            return

        logger.debug("configuring CouchDB clustering")
        setup_node = self.nodes[0]
        for node in self.nodes[1:]:
            setup_node.post(
                "/_cluster_setup",
                {
                    "action": "enable_cluster",
                    "bind_address": "0.0.0.0",
                    "username": username,
                    "password": password,
                    "node_count": str(len(self.nodes)),
                    "remote_node": node.private_address,
                    "remote_current_user": username,
                    "remote_current_password": password,
                },
            )

            setup_node.post(
                "/_cluster_setup",
                {
                    "action": "add_node",
                    "host": node.private_address,
                    "port": 5984,
                    "username": username,
                    "password": password,
                },
            )

            logger.debug(f"added node {node.private_address} to cluster")

        setup_node.post(
            "/_cluster_setup",
            {
                "action": "finish_cluster",
            },
        )

        logger.debug("finished configuring CouchDB clustering")

    def ok(self) -> bool:
        for node in self.nodes:
            if not node.ok():
                return False
        return True

    def is_setup(self) -> bool:
        expected = sorted([n.private_address for n in self.nodes])
        resp = self.get("/_membership")
        body = resp.json()
        actual = sorted(body["cluster_nodes"])
        for e, a in zip(expected, actual):
            if not a.endswith(e):
                return False
        return len(actual) == len(self.nodes)

    def post(self, path: str, json: dict | None = None) -> requests.Response:
        return self.default_node.post(path, json)

    def put(self, path: str, json: dict | None = None) -> requests.Response:
        return self.default_node.put(path, json)

    def get(self, path: str) -> requests.Response:
        return self.default_node.get(path)

    def delete(self, path: str) -> requests.Response:
        return self.default_node.delete(path)

    def db(self, name: str) -> DB:
        return self.default_node.db(name)

    def dbs(
        self, limit: int = 100, start_key: str | None = None, end_key: str | None = None
    ) -> Generator[DB, None, None]:
        return self.default_node.dbs(limit, start_key, end_key)

    def dbs_info(
        self, db_names: Iterable[str], page_size: int = 100
    ) -> Generator[DBInfo, None, None]:
        return self.default_node.dbs_info(db_names, page_size)

    def config(self) -> dict[str, str]:
        return self.default_node.config()

    def get_config(self, section: str, key: str | None = None) -> Any:
        return self.default_node.get_config(section, key)

    def set_config(self, section: str, key: str, value: str) -> None:
        return self.default_node.set_config(section, key, value)

    def membership(self) -> MembershipResponse:
        resp = self.get("/_membership")
        return resp.json()

    def get_node(self, i: int) -> Node | None:
        if i < 0 or i >= len(self.nodes):
            return None
        return self.nodes[i]

    def add_node(self, maintenance_mode: bool = False) -> Node:
        new_node = Node.create(self.name)
        new_node.reload()

        if maintenance_mode:
            new_node.set_config("couchdb", "maintenance_mode", "true")
        self.put(f"/_node/_local/_nodes/couchdb@{new_node.private_address}", json={})

        for node in self.nodes:
            try:
                new_node.put(
                    f"/_node/_local/_nodes/couchdb@{node.private_address}", json={}
                )
            except requests.exceptions.HTTPError as e:
                if e.response and e.response.status_code != 409:
                    raise e
        self.nodes.append(new_node)
        self.reorder_nodes()
        return new_node

    def seed(self, num_dbs: int, docs_per_db: int):
        def do(i):
            db = self.db(f"db-{i}").create()
            for i in range(docs_per_db):
                db.insert({"index": i})

        parallel_iter_with_progress(
            do,
            range(num_dbs),
            description="creating databases",
        )

    def validate_seed(self, num_dbs: int, docs_per_db: int):
        total = 0
        for info in self.dbs_info(
            (db.name for db in self.dbs(start_key="db-", end_key="db-\ufff0"))
        ):
            total += 1
            if info["info"]["doc_count"] != docs_per_db:
                raise Exception(
                    f"db {info['key']} has {info['info']['doc_count']} docs"
                )

        if total != num_dbs:
            raise Exception(f"expected {num_dbs} dbs, got {total}")

    def wait_for_seed(self, num_dbs: int, docs_per_db: int):
        while True:
            sleep(0.5)
            total = 0
            for info in self.dbs_info(
                (db.name for db in self.dbs(start_key="db-", end_key="db-\ufff0"))
            ):
                total += 1
                if info["info"]["doc_count"] != docs_per_db:
                    continue
            if total != num_dbs:
                continue
            break

    def destroy_seed_data(self):
        parallel_iter_with_progress(
            lambda db: db.destroy(),
            self.dbs(start_key="db-", end_key="db-\ufff0"),
            description="destroying databases",
        )
