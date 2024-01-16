from typing import TYPE_CHECKING, Any

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
        return f"{self.node.private_address}/{self.name}"

    def insert(self, doc: dict[str, Any]):
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

    def delete(self):
        self.node.delete(f"/{self.name}")

    def get(self):
        resp = self.node.get(f"/{self.name}")
        return resp.json()
