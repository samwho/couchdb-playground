from typing import TYPE_CHECKING

import requests
from couch.log import logger
from couch.types import SystemResponse
from docker.models.containers import Container

from .credentials import password, session, username
from .db import DB

if TYPE_CHECKING:
    from .cluster import Cluster


class Node:
    container: Container
    cluster: "Cluster"

    def __init__(self, container: Container):
        self.container = container

    def reload(self):
        self.container.reload()

    @property
    def local_address(self) -> str:
        port = self.container.ports["5984/tcp"][0]["HostPort"]
        return f"http://localhost:{port}"

    @property
    def private_address(self) -> str:
        return f"couchdb@{self.container.name}.cluster.local"

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
