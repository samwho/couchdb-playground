from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Generator, Iterable


def parallel_map[T, R](
    f: Callable[[T], R], iter: Iterable[T], parallelism=16
) -> Generator[R, None, None]:
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        return executor.map(f, iter)
