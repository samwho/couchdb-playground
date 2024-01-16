from typing import TYPE_CHECKING

import requests

from .credentials import password, session, username
from .db import DB

if TYPE_CHECKING:
    from .cluster import Cluster


class Node:
    cluster: "Cluster"
    local_address: str
    private_address: str

    def __init__(self, cluster, local_address, private_address):
        self.cluster = cluster
        self.local_address = local_address
        self.private_address = private_address

    def auth(self, username: str, password: str) -> requests.Response:
        url = f"{self.local_address}/_session"
        resp = session.post(url, json={"name": username, "password": password})
        resp.raise_for_status()
        return resp

    def __str__(self) -> str:
        return self.private_address

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
        resp.raise_for_status()
        return resp

    def post(self, path: str, json: dict | None = None) -> requests.Response:
        return self.request("POST", path, json)

    def put(self, path: str, json: dict | None = None) -> requests.Response:
        return self.request("PUT", path, json)

    def get(self, path: str) -> requests.Response:
        return self.request("GET", path)

    def total_dbs(self) -> int:
        resp = self.get("/_dbs")
        body = resp.json()
        return body["doc_count"]

    def create_db(self, name: str) -> DB:
        self.put(f"/{name}")
        return DB(self, name)

    def db(self, name: str) -> DB:
        return DB(self, name)

    def dbs(self) -> list[DB]:
        return [DB(self, name) for name in self.get("/_all_dbs").json()]
