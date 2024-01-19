from typing import TYPE_CHECKING, Any

from couch.types import DatabaseResponse

from .document import Document

if TYPE_CHECKING:
    from .node import Node


class DB:
    node: "Node"
    name: str

    def __init__(self, node: "Node", name: str):
        self.node = node
        self.name = name

    def __str__(self) -> str:
        return self.name

    def on_node(self, node: "Node") -> "DB":
        return DB(node, self.name)

    def create(self, q: int = 2, n: int = 2) -> "DB":
        self.node.put(f"/{self.name}?q={q}&n={n}")
        return self

    def insert(self, doc: dict[str, Any]) -> Document:
        resp = self.node.post(f"/{self.name}", json=doc)
        return Document.from_response(self, resp)

    def count(self) -> int:
        resp = self.node.get(f"/{self.name}")
        body = resp.json()
        return body["doc_count"]

    def list(self) -> list[Document]:
        resp = self.node.get(f"/{self.name}/_all_docs")
        body = resp.json()
        return [Document(self, row["id"], row["value"]["rev"]) for row in body["rows"]]

    def get(self, id: str) -> Document:
        resp = self.node.get(f"/{self.name}/{id}")
        return Document.from_response(self, resp)

    def destroy(self):
        self.node.delete(f"/{self.name}")

    def describe(self) -> DatabaseResponse:
        resp = self.node.get(f"/{self.name}")
        return resp.json()
