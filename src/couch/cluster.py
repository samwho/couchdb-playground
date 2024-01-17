from concurrent.futures import Future, ThreadPoolExecutor, as_completed

import click
import requests
from tqdm import tqdm

from .credentials import password, username
from .db import DB
from .node import Node
from .types import MembershipResponse


class Cluster:
    nodes: list[Node]
    default_node: Node

    def __init__(self):
        self.nodes = [
            Node(
                local_address="http://localhost:5984",
                private_address="couchdb1.cluster.local",
                cluster=self,
            ),
            Node(
                local_address="http://localhost:5985",
                private_address="couchdb2.cluster.local",
                cluster=self,
            ),
            Node(
                local_address="http://localhost:5986",
                private_address="couchdb3.cluster.local",
                cluster=self,
            ),
        ]

        self.default_node = self.nodes[0]

    def setup(self):
        click.echo("setting up cluster...")
        setup_node = self.default_node
        for node in self.nodes[1:]:
            click.echo(f"joining {node.private_address} to cluster...")
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

        click.echo("finishing cluster...")

        setup_node.post(
            "/_cluster_setup",
            {
                "action": "finish_cluster",
            },
        )

        setup_node.get("/_cluster_setup")
        click.echo("done")

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

    def get_node(self, name: str) -> Node | None:
        for node in cluster.nodes:
            if node.private_address == name:
                return node
            if node.local_address == name:
                return node
            if node.private_address.endswith(name):
                return node
            if node.private_address.startswith(name):
                return node
        return None

    def remove_node(self, node: Node):
        resp = self.get(f"/_node/_local/_nodes/couchdb@{node.private_address}")
        rev = resp.json()["_rev"]
        self.delete(f"/_node/_local/_nodes/couchdb@{node.private_address}?rev={rev}")

    def add_node(self, node: Node):
        self.put(f"/_node/_local/_nodes/couchdb@{node.private_address}", json={})

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


cluster = Cluster()
