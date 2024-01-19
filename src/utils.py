import functools
import random
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import timedelta
from itertools import islice
from typing import Callable, Generator, Iterable, Iterator
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    MofNCompleteColumn,
    TimeElapsedColumn,
)
from rich.console import Console

from couch.log import logger


def parallel_map[T, R](
    f: Callable[[T], R], iter: Iterable[T], parallelism=16
) -> Iterator[R]:
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        return executor.map(f, iter)


def progress(**kwargs) -> Progress:
    columns = [
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
    ]
    return Progress(*columns, expand=True, **kwargs)


@contextmanager
def status(text: str):
    console = Console()
    with console.status(f" {text}"):
        try:
            yield
        except Exception:
            console.print(f"❌ {text}")
            raise
    console.print(f"✅ {text}")


def parallel_map_with_progress[T, R](
    f: Callable[[T], R], iter: Iterable[T], parallelism=16, description: str = ""
) -> Generator[R, None, None]:
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        with progress() as pbar:
            task = pbar.add_task(description)
            total = 0
            futures = []
            for i in iter:
                total += 1
                pbar.update(task, total=total)
                futures.append(executor.submit(f, i))

            for future in futures:
                try:
                    yield future.result()
                except Exception:
                    pbar.update(task, description=f"❌ {description}")
                    raise
                pbar.update(task, advance=1)

            pbar.update(task, description=f"✅ {description}")


def parallel_iter_with_progress[T](
    f: Callable[[T], None], iter: Iterable[T], parallelism=16, description: str = ""
):
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        with progress() as pbar:
            task = pbar.add_task(description)
            futures = []
            total = 0
            for i in iter:
                total += 1
                pbar.update(task, total=total)
                futures.append(executor.submit(f, i))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    pbar.update(task, description=f"❌ {description}")
                    raise
                pbar.update(task, advance=1)

            pbar.update(task, description=f"✅ {description}")


def parallel_iter[T](f: Callable[[T], None], iter: Iterable[T], parallelism=16):
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = []
        for i in iter:
            futures.append(executor.submit(f, i))

        for future in as_completed(futures):
            future.result()


def random_string(length: int = 6) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def batched[T](iterable: Iterable[T], n: int) -> Generator[tuple[T, ...], None, None]:
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def bytes_to_human(bytes: int) -> str:
    if bytes < 1024:
        return f"{bytes}B"
    elif bytes < 1024**2:
        return f"{bytes / 1024:.1f}KB"
    elif bytes < 1024**3:
        return f"{bytes / 1024 ** 2:.1f}MB"
    else:
        return f"{bytes / 1024 ** 3:.1f}GB"


def duration_to_human(delta: timedelta) -> str:
    seconds = delta.total_seconds()
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 60 * 60:
        return f"{seconds / 60:.0f}m"
    elif seconds < 60 * 60 * 24:
        return f"{seconds / 60 / 60:.0f}h"
    else:
        return f"{seconds / 60 / 60 / 24:.0f}d"


def retries_enabled() -> bool:
    local = threading.local()
    if not hasattr(local, "retries_enabled"):
        local.retries_enabled = True
    return local.retries_enabled


def disable_retries():
    local = threading.local()
    if not hasattr(local, "retries_enabled"):
        local.retries_enabled = True
    local.retries_enabled = False


def enable_retries():
    local = threading.local()
    if not hasattr(local, "retries_enabled"):
        local.retries_enabled = True
    local.retries_enabled = True


def retry(max_attempts: int = 3, initial_wait: float = 1, backoff_factor: float = 2):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            wait_time = initial_wait
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception:
                    attempts += 1
                    if not retries_enabled():
                        logger.debug("retries disabled, not retrying")
                        raise
                    if attempts == max_attempts:
                        logger.debug("max retries reached, not retrying")
                        raise
                    time.sleep(wait_time + random.uniform(0, wait_time))
                    wait_time *= backoff_factor

        return wrapper

    return decorator


@contextmanager
def no_retries():
    prev = retries_enabled()
    disable_retries()
    try:
        yield
    finally:
        if prev:
            enable_retries()
        else:
            disable_retries()
