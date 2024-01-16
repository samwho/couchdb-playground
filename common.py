import requests
from requests.adapters import HTTPAdapter, Retry

DEBUG = False

session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[404, 500, 502, 503, 504])

session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))

num_dbs = 2000

nodes = [
    {
        "local_address": "http://localhost:5984",
        "private_address": "couchdb1.cluster.local",
    },
    {
        "local_address": "http://localhost:5985",
        "private_address": "couchdb2.cluster.local",
    },
    {
        "local_address": "http://localhost:5986",
        "private_address": "couchdb3.cluster.local",
    },
]

username = "admin"
password = "password"


def debug(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)


def auth(node: int, username: str, password: str) -> requests.Response:
    url = f"{nodes[node]['local_address']}/_session"
    debug(f"POST {url}")
    resp = session.post(url, json={"name": username, "password": password})
    debug(resp.text)
    resp.raise_for_status()
    return resp


def crequest(
    method: str, node: int, path: str, json: dict | None = None
) -> requests.Response:
    url = f"{nodes[node]['local_address']}{path}"
    debug(f"{method} {url}")
    resp = session.request(method, url, json=json)
    if resp.status_code == 401:
        auth(node, username, password)
        return crequest(method, node, path, json)
    debug(resp.text)
    resp.raise_for_status()
    return resp


def cpost(node: int, path: str, json: dict | None = None) -> requests.Response:
    return crequest("POST", node, path, json)


def cput(node: int, path: str, json: dict | None = None) -> requests.Response:
    return crequest("PUT", node, path, json)


def cget(node: int, path: str) -> requests.Response:
    return crequest("GET", node, path)


def is_cluster_setup() -> bool:
    expected = sorted([n["private_address"] for n in nodes])
    resp = cget(0, "/_membership")
    body = resp.json()
    actual = sorted(body["cluster_nodes"])
    for e, a in zip(expected, actual):
        if not a.endswith(e):
            return False
    return len(actual) == len(nodes)
