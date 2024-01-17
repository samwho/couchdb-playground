from typing import TYPE_CHECKING, Any

from couch.types import DatabaseResponse

from .document import Document

if TYPE_CHECKING:
    from .cluster import Cluster
    from .node import Node


class DB:
    cluster: "Cluster"
    node: "Node"
    name: str

    def __init__(self, cluster: "Cluster", name: str):
        self.cluster = cluster
        self.node = cluster.default_node
        self.name = name

    def __str__(self) -> str:
        return self.name

    def on_node(self, node: "Node") -> "DB":
        db = DB(self.cluster, self.name)
        db.node = node
        return db

    def insert(self, doc: dict[str, Any]) -> Document:
        resp = self.cluster.post(f"/{self.name}", json=doc)
        return Document.from_response(self, resp)

    def count(self) -> int:
        resp = self.cluster.get(f"/{self.name}")
        body = resp.json()
        return body["doc_count"]

    def list(self) -> list[Document]:
        resp = self.cluster.get(f"/{self.name}/_all_docs")
        body = resp.json()
        return [Document(self, row["id"], row["value"]["rev"]) for row in body["rows"]]

    def delete(self):
        self.cluster.delete(f"/{self.name}")

    def get(self) -> DatabaseResponse:
        resp = self.cluster.get(f"/{self.name}")
        return resp.json()
