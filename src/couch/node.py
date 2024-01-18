from datetime import datetime, timedelta
from uuid import uuid4
import docker
from typing import TYPE_CHECKING

import requests
from couch.log import logger
from couch.types import MembershipResponse, SystemResponse
from docker.models.containers import Container

from .credentials import password, session, username
from .db import DB

if TYPE_CHECKING:
    from .cluster import Cluster


class Node:
    container: Container
    cluster: "Cluster"
    index: int

    def __init__(self, index: int, container: Container):
        self.index = index
        self.container = container

    def reload(self):
        self.container.reload()

    @staticmethod
    def create(cluster_name: str) -> "Node":
        client = docker.from_env()
        id = uuid4()
        node_name = f"cpg-{cluster_name}-{id}"
        client.volumes.create(name=node_name, labels={"cpg": cluster_name})

        container = client.containers.run(
            "couchdb:3.2",
            name=node_name,
            hostname=f"{node_name}.cluster.local",
            detach=True,
            network=f"cpg-{cluster_name}",
            labels={"cpg": cluster_name},
            ports={"5984/tcp": ("127.0.0.1", None)},
            volumes={node_name: {"bind": "/opt/couchdb/data", "mode": "rw"}},
            environment={
                "COUCHDB_USER": username,
                "COUCHDB_PASSWORD": password,
                "ERL_FLAGS": f"-name couchdb@{node_name}.cluster.local -setcookie brumbrum -kernel inet_dist_listen_min 9100 -kernel inet_dist_listen_max 9200",
            },
        )

        return Node(0, container)

    @property
    def local_address(self) -> str:
        port = self.container.ports["5984/tcp"][0]["HostPort"]
        return f"http://localhost:{port}"

    @property
    def private_address(self) -> str:
        return f"{self.container.name}.cluster.local"

    @property
    def name(self) -> str:
        return self.container.name

    def started_at(self) -> datetime:
        started_at = self.container.attrs["State"]["StartedAt"]
        return datetime.fromisoformat(started_at[:-4])

    def uptime(self) -> timedelta:
        return datetime.now() - self.started_at()

    def restart(self):
        self.container.restart()

    def destroy(self, remove=True, keep_data=False):
        if remove:
            self.remove()

        client = docker.from_env()
        self.container.stop()
        self.container.remove()

        if not keep_data:
            client.volumes.get(self.container.name).remove()

    def remove(self):
        resp = self.cluster.get(f"/_node/_local/_nodes/couchdb@{self.private_address}")
        rev = resp.json()["_rev"]
        self.cluster.delete(
            f"/_node/_local/_nodes/couchdb@{self.private_address}?rev={rev}"
        )
        self.cluster.nodes.remove(self)
        self.cluster.reorder_nodes()

    def auth(self, username: str, password: str) -> requests.Response:
        logger.debug(f"authenticating as {username}")
        url = f"{self.local_address}/_session"
        resp = session.post(url, json={"name": username, "password": password})
        resp.raise_for_status()
        return resp

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Node):
            return False
        return self.local_address == __value.local_address

    def __hash__(self) -> int:
        return hash(self.local_address)

    def request(
        self, method: str, path: str, json: dict | None = None
    ) -> requests.Response:
        url = f"{self.local_address}{path}"
        resp = session.request(method, url, json=json)
        if resp.status_code == 401:
            self.auth(username, password)
            return self.request(method, path, json)
        logger.debug(f"{method} {url} {resp.status_code}")
        if resp.status_code >= 400:
            logger.debug(f"  body: {resp.text}")
        resp.raise_for_status()
        return resp

    def post(self, path: str, json: dict | None = None) -> requests.Response:
        return self.request("POST", path, json)

    def put(self, path: str, json: dict | None = None) -> requests.Response:
        return self.request("PUT", path, json)

    def get(self, path: str) -> requests.Response:
        return self.request("GET", path)

    def delete(self, path: str) -> requests.Response:
        return self.request("DELETE", path)

    def ok(self) -> bool:
        try:
            self.get("/_up")
            return True
        except requests.exceptions.ConnectionError:
            return False

    def membership(self) -> MembershipResponse:
        resp = self.get("/_membership")
        return resp.json()

    def total_dbs(self) -> int:
        resp = self.get("/_dbs")
        body = resp.json()
        return body["doc_count"]

    def create_db(self, name: str, q: int = 2, n: int = 2) -> DB:
        self.put(f"/{name}?q={q}&n={n}")
        return DB(self.cluster, name)

    def db(self, name: str) -> DB:
        return DB(self.cluster, name)

    def dbs(self) -> list[DB]:
        return [DB(self.cluster, name) for name in self.get("/_all_dbs").json()]

    def system(self) -> SystemResponse:
        return self.get("/_node/_local/_system").json()
