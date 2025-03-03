from contextlib import contextmanager
from typing import Generator, Any


@contextmanager
def noop_async_tracer(operation_name: str, operation_kwargs: dict) -> Generator[None, Any, None]:
    yield