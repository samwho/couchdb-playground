from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from node import Node


class DB:
    node: "Node"
    name: str

    def __init__(self, node: "Node", name: str):
        self.node = node
        self.name = name

    def __str__(self) -> str:
        return f"{self.node.private_address}/{self.name}"

    def insert(self, doc: dict[str, Any]):
        self.node.post(f"/{self.name}", json=doc)

    def count(self) -> int:
        resp = self.node.get(f"/{self.name}")
        body = resp.json()
        return body["doc_count"]
