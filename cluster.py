from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from tqdm import tqdm
import requests
import click
from db import DB
from node import Node
from credentials import username, password


class Cluster:
    nodes: list[Node]

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

    def setup(self):
        click.echo("setting up cluster...")
        setup_node = self.nodes[0]
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
        return self.nodes[0].post(path, json)

    def put(self, path: str, json: dict | None = None) -> requests.Response:
        return self.nodes[0].put(path, json)

    def get(self, path: str) -> requests.Response:
        return self.nodes[0].get(path)

    def db(self, name: str) -> DB:
        return self.nodes[0].db(name)

    def create_seed_data(self, num_dbs: int = 2000):
        print("creating seed data...")
        node = self.nodes[0]
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

    def wait_for_sync(self):
        print("waiting for replication...")
        for node in self.nodes:
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

    def assert_all_dbs_have_one_doc(self, num_dbs: int = 2000):
        for node in self.nodes:
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

    def all_missing_dbs(self) -> dict[Node, set[str]]:
        with ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
            futures: list[Future[list[str]]] = []
            for node in self.nodes:
                futures.append(executor.submit(node.all_dbs))

            dbs: dict[Node, set[str]] = {}
            for i, future in enumerate(futures):
                dbs[self.nodes[i]] = set(future.result())

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
