import threading

import requests
from couch.credentials import password, username
from couch.log import logger
from utils import retry


def session() -> requests.Session:
    local = threading.local()
    if not hasattr(local, "session"):
        local.session = requests.Session()
    return local.session


class HTTPMixin:
    def base_url(self) -> str:
        raise NotImplementedError

    def request(
        self,
        method: str,
        path: str,
        json: dict | None = None,
        max_attempts: int = 3,
        initial_wait: float = 1,
        backoff_factor: float = 2,
    ) -> requests.Response:
        url = f"{self.base_url()}{path}"

        @retry(max_attempts, initial_wait, backoff_factor)
        def req():
            print(f"{method} {url}")
            resp = session().request(method, url, json=json)
            if resp.status_code == 401:
                resp = session().post(
                    f"{self.base_url()}/_session",
                    json={"name": username, "password": password},
                )
                resp.raise_for_status()
                resp = session().request(method, url, json=json)
            logger.debug(f"{method} {url} {resp.status_code}")
            if resp.status_code >= 400:
                logger.debug(f"  body: {resp.text}")
            resp.raise_for_status()
            return resp

        return req()

    def get(
        self,
        path: str,
        max_attempts: int = 3,
        initial_wait: float = 1,
        backoff_factor: float = 2,
    ) -> requests.Response:
        return self.request(
            "GET",
            path,
            json=None,
            max_attempts=max_attempts,
            initial_wait=initial_wait,
            backoff_factor=backoff_factor,
        )

    def post(
        self,
        path: str,
        json: dict | None = None,
        max_attempts: int = 3,
        initial_wait: float = 1,
        backoff_factor: float = 2,
    ) -> requests.Response:
        return self.request(
            "POST", path, json, max_attempts, initial_wait, backoff_factor
        )

    def put(
        self,
        path: str,
        json: dict | None = None,
        max_attempts: int = 3,
        initial_wait: float = 1,
        backoff_factor: float = 2,
    ) -> requests.Response:
        return self.request(
            "PUT", path, json, max_attempts, initial_wait, backoff_factor
        )

    def delete(
        self,
        path: str,
        max_attempts: int = 3,
        initial_wait: float = 1,
        backoff_factor: float = 2,
    ) -> requests.Response:
        return self.request(
            "DELETE",
            path,
            json=None,
            max_attempts=max_attempts,
            initial_wait=initial_wait,
            backoff_factor=backoff_factor,
        )
