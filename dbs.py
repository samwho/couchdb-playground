import requests
from concurrent.futures import ThreadPoolExecutor
from common import cget, cput, nodes


def get_missing_dbs() -> dict[int, set[str]]:
    with ThreadPoolExecutor(max_workers=16) as executor:

        def get_all_dbs(node: int) -> list[str]:
            resp = cget(node, "/_all_dbs")
            return resp.json()

        futures = []
        for node in range(len(nodes)):
            futures.append(executor.submit(get_all_dbs, node))

        dbs = {}
        for i, future in enumerate(futures):
            dbs[i] = set(future.result())

        all_dbs = set()
        for node in dbs:
            all_dbs |= dbs[node]

        missing_dbs = {}
        for node in dbs:
            missing_dbs[node] = all_dbs - dbs[node]

        return missing_dbs


def get_missing_db() -> tuple[int, str]:
    missing = get_missing_dbs()
    for node, dbs in missing.items():
        if len(dbs) > 0:
            return node, dbs.pop()
    return -1, ""


def get_total_dbs() -> int:
    resp = cget(0, "/_dbs")
    body = resp.json()
    return body["doc_count"]


def get_doc_count(node: int, db: str) -> int:
    resp = cget(node, f"/{db}")
    body = resp.json()
    return body["doc_count"]


def create_db(node: int, db: str) -> requests.Response:
    return cput(node, f"/{db}")
