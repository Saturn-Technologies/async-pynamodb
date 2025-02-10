import asyncio
from typing import Any, AsyncIterator, Callable, Coroutine, Iterable, TypeVar

from pynamodb.constants import (
    CAMEL_COUNT,
    CAPACITY_UNITS,
    CONSUMED_CAPACITY,
    ITEMS,
    LAST_EVALUATED_KEY,
    SCANNED_COUNT,
    TOTAL,
)
from pynamodb.pagination import RateLimiter

_T = TypeVar("_T")


class AsyncRateLimiter(RateLimiter):
    async def acquire(self) -> None:
        time_to_sleep = max(
            0,
            self._consumed / float(self.rate_limit)
            - (self._time_module.time() - self._time_of_last_acquire),
        )
        await asyncio.sleep(time_to_sleep)
        self._consumed = 0
        self._time_of_last_acquire = self._time_module.time()


class AsyncPageIterator(AsyncIterator[_T]):
    def __init__(
        self,
        operation: Callable[..., Coroutine],
        args: tuple,
        kwargs: dict[str, Any],
        rate_limit: float | None = None,
    ) -> None:
        self._operation = operation
        self._args = args
        self._kwargs = kwargs
        self._last_evaluated_key = kwargs.get("exclusive_start_key")
        self._is_last_page = False
        self._total_scanned_count = 0
        self._rate_limiter = None
        if rate_limit:
            self._rate_limiter = AsyncRateLimiter(rate_limit)

    def __aiter__(self) -> AsyncIterator[_T]:
        return self

    async def __anext__(self) -> _T:
        if self._is_last_page:
            raise StopAsyncIteration()

        self._kwargs["exclusive_start_key"] = self._last_evaluated_key
        if self._rate_limiter:
            await self._rate_limiter.acquire()
            self._kwargs["return_consumed_capacity"] = TOTAL

        page = await self._operation(*self._args, **self._kwargs)
        self._last_evaluated_key = page.get(LAST_EVALUATED_KEY)
        self._is_last_page = self._last_evaluated_key is None
        self._total_scanned_count += page[SCANNED_COUNT]
        if self._rate_limiter:
            consumed_capacity = page.get(CONSUMED_CAPACITY, {}).get(CAPACITY_UNITS, 0)
            self._rate_limiter.consume(consumed_capacity)
        return page

    async def next(self) -> _T:
        return await self.__anext__()

    @property
    def key_names(self) -> Iterable[str]:
        # If the current page has a last_evaluated_key, use it to determine key attributes
        if self._last_evaluated_key:
            return self._last_evaluated_key.keys()

        # Use the table meta data to determine the key attributes
        table_meta = self._operation.__self__.get_meta_table()  # type: ignore
        return table_meta.get_key_names(self._kwargs.get("index_name"))

    @property
    def page_size(self) -> int | None:
        return self._kwargs.get("limit")

    @page_size.setter
    def page_size(self, page_size: int) -> None:
        self._kwargs["limit"] = page_size

    @property
    def last_evaluated_key(self) -> dict[str, dict[str, Any]] | None:
        return self._last_evaluated_key

    @property
    def total_scanned_count(self) -> int:
        return self._total_scanned_count


class AsyncResultIterator(AsyncIterator[_T]):
    def __init__(
        self,
        operation: Callable[..., Coroutine],
        args: tuple,
        kwargs: dict,
        map_fn: Callable | None = None,
        limit: int | None = None,
        rate_limit: float | None = None,
    ) -> None:
        self.page_iter: AsyncPageIterator[_T] = AsyncPageIterator(
            operation, args, kwargs, rate_limit
        )
        self._map_fn = map_fn
        self._limit = limit
        self._total_count = 0
        self._index = 0
        self._count = 0

    async def _get_next_page(self) -> None:
        page = await self.page_iter.next()
        self._count = page[CAMEL_COUNT]
        self._items = page.get(ITEMS)  # not returned if 'Select' is set to 'COUNT'
        self._index = 0 if self._items else self._count
        self._total_count += self._count

    def __aiter__(self) -> AsyncIterator[_T]:
        return self

    async def __anext__(self) -> _T:
        if self._limit == 0:
            raise StopAsyncIteration

        while self._index == self._count:
            await self._get_next_page()

        item = self._items[self._index]
        self._index += 1
        if self._limit is not None:
            self._limit -= 1
        if self._map_fn:
            item = self._map_fn(item)
        return item

    async def next(self) -> _T:
        return await self.__anext__()

    @property
    def last_evaluated_key(self) -> dict[str, dict[str, Any]] | None:
        if self._index == self._count:
            # Not started iterating yet: return `exclusive_start_key` if set, otherwise expect None; or,
            # Entire page has been consumed: last_evaluated_key is whatever DynamoDB returned
            # It may correspond to the current item, or it may correspond to an item evaluated but not returned.
            return self.page_iter.last_evaluated_key

        # In the middle of a page of results: reconstruct a last_evaluated_key from the current item
        # The operation should be resumed starting at the last item returned, not the last item evaluated.
        # This can occur if the 'limit' is reached in the middle of a page.
        item = self._items[self._index - 1]
        return {key: item[key] for key in self.page_iter.key_names}

    @property
    def total_count(self) -> int:
        return self._total_count
