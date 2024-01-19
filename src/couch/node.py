from datetime import datetime, timedelta
from time import sleep
from typing import TYPE_CHECKING, Any, Generator, Iterable, cast

import docker
import requests
from couch.types import DBInfo, MembershipResponse, SystemResponse
from docker.models.containers import Container
from couch.http import HTTPMixin
from utils import batched, random_string

from .credentials import password, username
from .db import DB

if TYPE_CHECKING:
    from .cluster import Cluster


class Node(HTTPMixin):
    _container: Container
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
        id = random_string()
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

        return Node(0, cast(Container, container))

    @property
    def local_address(self) -> str:
        port = self.container.ports["5984/tcp"][0]["HostPort"]
        return f"http://localhost:{port}"

    @property
    def private_address(self) -> str:
        return f"{self.container.name}.cluster.local"

    @property
    def name(self) -> str:
        return self.container.name  # type: ignore

    def get_config(self, section: str, key: str | None = None) -> Any:
        if key:
            return self.get(f"/_node/_local/_config/{section}/{key}").json()
        else:
            return self.get(f"/_node/_local/_config/{section}").json()

    def set_config(self, section: str, key: str, value: Any) -> None:
        self.put(f"/_node/_local/_config/{section}/{key}", json=value)

    def config(self) -> dict[str, Any]:
        return self.get("/_node/_local/_config").json()

    def started_at(self) -> datetime:
        started_at = self.container.attrs["State"]["StartedAt"]  # type: ignore
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
            client.volumes.get(self.container.name).remove()  # type: ignore

    def remove(self):
        try:
            resp = self.cluster.get(
                f"/_node/_local/_nodes/couchdb@{self.private_address}"
            )
            rev = resp.json()["_rev"]
            self.cluster.delete(
                f"/_node/_local/_nodes/couchdb@{self.private_address}?rev={rev}"
            )
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                pass
            else:
                raise e
        self.cluster.nodes.remove(self)
        self.cluster.reorder_nodes()

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Node):
            return False
        return self.local_address == __value.local_address

    def __hash__(self) -> int:
        return hash(self.local_address)

    def base_url(self) -> str:
        return self.local_address

    def ok(self) -> bool:
        try:
            self.get("/_up")
            return True
        except requests.exceptions.ConnectionError:
            return False
        except requests.exceptions.HTTPError:
            return False

    def membership(self) -> MembershipResponse:
        resp = self.get("/_membership")
        return resp.json()

    def total_dbs(self) -> int:
        resp = self.get("/_dbs")
        body = resp.json()
        return body["doc_count"]

    def db(self, name: str) -> DB:
        return DB(self, name)

    def dbs(
        self,
        page_size: int = 100,
        start_key: str | None = None,
        end_key: str | None = None,
    ) -> Generator[DB, None, None]:
        while True:
            url = f"/_all_dbs?limit={page_size + 1}"
            if start_key:
                url += f'&startkey="{start_key}"'
            if end_key:
                url += f'&endkey="{end_key}"'
            names = self.get(url).json()

            for name in names[:page_size]:
                yield DB(self, name)

            if len(names) == page_size + 1:
                start_key = names[-1]
            else:
                break

    def dbs_info(
        self, db_names: Iterable[str], page_size: int = 100
    ) -> Generator[DBInfo, None, None]:
        for batch in batched(db_names, page_size):
            infos: list[DBInfo] = self.post("/_dbs_info", json={"keys": batch}).json()
            for info in infos:
                yield info

    def system(self) -> SystemResponse:
        return self.get("/_node/_local/_system").json()

    def validate_seed(self, num_dbs: int, docs_per_db: int):
        total = 0
        for info in self.dbs_info(
            (db.name for db in self.dbs(start_key="db-", end_key="db-\ufff0"))
        ):
            if "error" in info:
                continue
            total += 1
            if info["info"]["doc_count"] != docs_per_db:
                raise Exception(f"{info['key']} has {info['info']['doc_count']} docs")

        if total != num_dbs:
            raise Exception(f"expected {num_dbs} dbs, got {total}")

    def wait_for_seed(self, num_dbs: int, docs_per_db: int, timeout: int = 60):
        start = datetime.now()
        while True:
            elapsed = (datetime.now() - start).total_seconds()
            if elapsed > timeout:
                raise Exception(
                    f"timed out waiting for seed data to be created (elapsed={elapsed}s)"
                )
            sleep(0.5)
            total = 0
            for info in self.dbs_info(
                (db.name for db in self.dbs(start_key="db-", end_key="db-\ufff0"))
            ):
                total += 1
                if info["info"]["doc_count"] != docs_per_db:
                    continue
            if total != num_dbs:
                continue
            break
