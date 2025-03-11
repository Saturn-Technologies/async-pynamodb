import asyncio
import typing

from typing import Any, TypeVar

from more_itertools import chunked

from pynamodb.constants import BATCH_GET_PAGE_LIMIT

if typing.TYPE_CHECKING:
    from pynamodb.models import Model # noqa: F401

_T = TypeVar("_T", bound="Model")

class BatchGetIterator(typing.AsyncIterator[_T]):

    def __init__(self, model_cls: typing.Type[_T], keys: typing.Iterable, consistent_read: bool | None = None, attributes_to_get: typing.Sequence[str] | None = None):
        self.model_cls = model_cls
        self.unprocessed_batch_items = list(keys)
        self.consistent_read = consistent_read
        self.attributes_to_get = attributes_to_get
        self.current_batch: list[dict[str, Any]] = []

    def __aiter__(self):
        return self

    def _get_coroutines(self, unprocessed_batch_items) -> list[typing.Coroutine[Any, Any, Any]]:
        """Split unprocessed batch items into chunks and create coroutines for each chunk."""
        serialized_keys = list(self.model_cls._batch_serialize_keys(unprocessed_batch_items))
        return [
            self.model_cls._async_batch_get_item(
                chunk, self.consistent_read, self.attributes_to_get
            )
            for chunk in chunked(serialized_keys, BATCH_GET_PAGE_LIMIT)
        ]

    @property
    def timeout(self) -> int:
        return self.model_cls.Meta.connect_timeout_seconds + self.model_cls.Meta.read_timeout_seconds

    async def __anext__(self) -> _T:
        # First check if we have items awaiting to be processed
        if self.current_batch:
            item = self.current_batch.pop(0)
            await asyncio.sleep(0)
            return self.model_cls.from_raw_data(item)

        # Check if we have unprocessed items
        if self.unprocessed_batch_items:
            # Process next batch
            futures: list[asyncio.Task] = []
            async with asyncio.TaskGroup() as tg:
                for coro in self._get_coroutines(self.unprocessed_batch_items):
                    future = tg.create_task(coro)
                    futures.append(future)
                    await asyncio.sleep(0)
            self.unprocessed_batch_items = []
            for future in futures:
                for items, unprocessed_keys in future.result():
                    # Add unprocessed items back to the queue
                    if unprocessed_keys:
                        self.unprocessed_batch_items.extend(unprocessed_keys)
                    if items:
                        self.current_batch.extend(items)
                    await asyncio.sleep(0)
            if self.current_batch or self.unprocessed_batch_items:
                return await self.__anext__()
        raise StopAsyncIteration
