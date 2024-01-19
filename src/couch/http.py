import requests
from couch.credentials import password, username
from couch.log import logger
from utils import retry

session = requests.Session()


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
        timeout: float = 5,
    ) -> requests.Response:
        url = f"{self.base_url()}{path}"

        @retry(max_attempts, initial_wait, backoff_factor)
        def req():
            resp = session.request(method, url, json=json, timeout=timeout)
            if resp.status_code == 401:
                resp = session.post(
                    f"{self.base_url()}/_session",
                    json={"name": username, "password": password},
                    timeout=5,
                )
                resp.raise_for_status()
                resp = session.request(method, url, json=json, timeout=timeout)
            logger.debug(f"{method} {url} {resp.status_code}")
            if not resp.ok:
                logger.debug(resp.text)
            resp.raise_for_status()
            return resp

        return req()

    def get(
        self,
        path: str,
        max_attempts: int = 3,
        initial_wait: float = 1,
        backoff_factor: float = 2,
        timeout: float = 5,
    ) -> requests.Response:
        return self.request(
            "GET",
            path,
            json=None,
            max_attempts=max_attempts,
            initial_wait=initial_wait,
            backoff_factor=backoff_factor,
            timeout=timeout,
        )

    def post(
        self,
        path: str,
        json: dict | None = None,
        max_attempts: int = 3,
        initial_wait: float = 1,
        backoff_factor: float = 2,
        timeout: float = 5,
    ) -> requests.Response:
        return self.request(
            "POST", path, json, max_attempts, initial_wait, backoff_factor, timeout
        )

    def put(
        self,
        path: str,
        json: dict | None = None,
        max_attempts: int = 3,
        initial_wait: float = 1,
        backoff_factor: float = 2,
        timeout: float = 5,
    ) -> requests.Response:
        return self.request(
            "PUT", path, json, max_attempts, initial_wait, backoff_factor, timeout
        )

    def delete(
        self,
        path: str,
        max_attempts: int = 3,
        initial_wait: float = 1,
        backoff_factor: float = 2,
        timeout: float = 5,
    ) -> requests.Response:
        return self.request(
            "DELETE",
            path,
            json=None,
            max_attempts=max_attempts,
            initial_wait=initial_wait,
            backoff_factor=backoff_factor,
            timeout=timeout,
        )
