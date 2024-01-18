from concurrent.futures import Future, ThreadPoolExecutor
from time import sleep
from typing import cast

import click
import docker
import requests
from couch.log import logger
from docker.models.containers import Container

from utils import parallel_map

from .credentials import password, username
from .db import DB
from .node import Node
from .types import MembershipResponse

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
        while not cluster.ok():
            sleep(1)

        cluster.setup()
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

    def dbs(self) -> list[DB]:
        return self.default_node.dbs()

    def membership(self) -> MembershipResponse:
        resp = self.get("/_membership")
        return resp.json()

    def get_node(self, i: int) -> Node | None:
        return self.nodes[i]

    def add_node(self) -> Node:
        new_node = Node.create(self.name)
        self.put(f"/_node/_local/_nodes/couchdb@{new_node.private_address}", json={})

        new_node.reload()
        for node in self.nodes:
            try:
                new_node.put(
                    f"/_node/_local/_nodes/couchdb@{node.private_address}", json={}
                )
            except requests.exceptions.HTTPError as e:
                if e.response.status_code != 409:
                    raise e
        self.nodes.append(new_node)
        self.reorder_nodes()
        return new_node

    def create_db(self, name: str, q: int = 2, n: int = 2) -> DB:
        return self.default_node.create_db(name, q=q, n=n)

    def all_missing_dbs(self) -> dict[Node, set[str]]:
        with ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
            futures: list[Future[list[DB]]] = []
            for node in self.nodes:
                futures.append(executor.submit(node.dbs))

            dbs: dict[Node, set[str]] = {}
            for i, future in enumerate(futures):
                dbs[self.nodes[i]] = set([db.name for db in future.result()])

            all_dbs = set()
            for node in dbs:
                all_dbs |= dbs[node]

            missing_dbs = {}
            for node in dbs:
                missing_dbs[node] = all_dbs - dbs[node]

            return missing_dbs

    def random_missing_db(self) -> DB | None:
        for node, dbs in self.all_missing_dbs().items():
            if len(dbs) > 0:
                return node.db(dbs.pop())
        return None
