from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from .db import DB


class Document:
    db: "DB"
    id: str
    rev: str

    @staticmethod
    def from_response(db: "DB", resp: requests.Response):
        body = resp.json()
        return Document(db, body["id"], body["rev"])

    def __init__(self, db: "DB", id: str, rev: str):
        self.db = db
        self.id = id
        self.rev = rev

    def __str__(self) -> str:
        return f"{self.db}/{self.id}"

    @property
    def body(self) -> dict[str, Any]:
        resp = self.db.node.get(f"/{self.db.name}/{self.id}")
        return resp.json()
