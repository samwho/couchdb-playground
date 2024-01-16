import requests
from requests.adapters import HTTPAdapter, Retry  # type: ignore

session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[404, 500, 502, 503, 504])

session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))

username = "admin"
password = "password"
