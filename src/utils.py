import functools
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from itertools import islice
from typing import Callable, Generator, Iterable, Iterator

from tqdm import tqdm


def parallel_map[T, R](
    f: Callable[[T], R], iter: Iterable[T], parallelism=16
) -> Iterator[R]:
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        return executor.map(f, iter)


def parallel_map_with_progress[T, R](
    f: Callable[[T], R], iter: Iterable[T], parallelism=16, description: str = ""
) -> Generator[R, None, None]:
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        with tqdm() as pbar:
            futures = []
            for i in iter:
                futures.append(executor.submit(f, i))

            pbar.total = len(futures)
            if description:
                pbar.set_description(description)
            for future in futures:
                yield future.result()
                pbar.update(1)


def parallel_iter_with_progress[T](
    f: Callable[[T], None], iter: Iterable[T], parallelism=16, description: str = ""
):
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        with tqdm() as pbar:
            futures = []
            for i in iter:
                futures.append(executor.submit(f, i))

            pbar.total = len(futures)
            if description:
                pbar.set_description(description)
            for future in futures:
                future.result()
                pbar.update(1)


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


def retry(max_attempts=3, initial_wait=1, backoff_factor=2):
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
                    if attempts == max_attempts:
                        raise
                    time.sleep(wait_time + random.uniform(0, wait_time))
                    wait_time *= backoff_factor

        return wrapper

    return decorator
