from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from .db import DB
    from .node import Node


class Document:
    db: "DB"
    id: str
    rev: str
    node: "Node"

    @staticmethod
    def from_response(db: "DB", resp: requests.Response):
        body = resp.json()
        return Document(db, body["id"], body["rev"])

    def __init__(self, db: "DB", id: str, rev: str):
        self.db = db
        self.node = db.node
        self.id = id
        self.rev = rev

    def __str__(self) -> str:
        return f"{self.db}/{self.id}"

    def get(self) -> dict[str, Any]:
        resp = self.node.get(f"/{self.db.name}/{self.id}")
        return resp.json()

    def delete(self):
        self.node.delete(f"/{self.db.name}/{self.id}?rev={self.rev}")

    def update(self, body: dict[str, Any]):
        resp = self.node.put(f"/{self.db.name}/{self.id}?rev={self.rev}", json=body)
        return Document.from_response(self.db, resp)

    def on_node(self, node: "Node") -> "Document":
        doc = Document(self.db, self.id, self.rev)
        doc.node = node
        return doc
