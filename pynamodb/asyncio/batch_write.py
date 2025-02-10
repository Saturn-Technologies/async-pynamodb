import typing

import asyncio

from aioitertools.asyncio import as_completed
from more_itertools.more import ichunked

from pynamodb.constants import (
    BATCH_WRITE_PAGE_LIMIT,
    PUT,
    DELETE,
    PUT_REQUEST,
    DELETE_REQUEST,
    UNPROCESSED_ITEMS,
)
from pynamodb.exceptions import PutError

_T = typing.TypeVar("_T")
BatchOperation = typing.Dict[str, typing.Any]
SerializedItem = typing.Dict[str, typing.Any]


class AsyncBatchWrite(typing.Generic[_T], typing.AsyncContextManager[typing.Self]):
    """
    Async context manager for batch write operations in DynamoDB.

    This class provides an interface for batched write operations (put/delete)
    that are executed asynchronously when the context manager exits.
    """

    def __init__(self, model: typing.Type[_T], auto_commit: bool = True):
        self.model = model
        self.max_operations = BATCH_WRITE_PAGE_LIMIT
        self.pending_operations: typing.List[BatchOperation] = []
        self.failed_operations: typing.List[typing.Any] = []
        self.auto_commit = auto_commit

    async def save(self, put_item: _T) -> None:
        """
        Queue an item for batch insertion.

        Args:
            put_item: The model instance to be inserted

        Raises:
            ValueError: If the model uses versioning
        """
        if put_item._version_attribute_name is not None:
            raise ValueError(
                "batch_write does not support versioned models. Use a transaction instead"
            )
        if len(self.pending_operations) == self.max_operations:
            if self.auto_commit:
                await self.commit()
        self.pending_operations.append({"action": PUT, "item": put_item})

    async def delete(self, del_item: _T) -> None:
        """
        Queue an item for batch deletion.

        Args:
            del_item: The model instance to be deleted

        Raises:
            ValueError: If the model uses versioning
        """
        if del_item._version_attribute_name is not None:
            raise ValueError(
                "batch_write does not support versioned models. Use a transaction instead"
            )
        if len(self.pending_operations) == self.max_operations:
            if self.auto_commit:
                await self.commit()
        self.pending_operations.append({"action": DELETE, "item": del_item})

    async def __aenter__(self) -> "AsyncBatchWrite[_T]":
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        exc_val: typing.Optional[BaseException],
        exc_tb: typing.Optional[typing.Any],
    ) -> None:
        """Exit the async context manager and commit pending operations."""
        await self.commit()

    async def commit(self) -> None:
        """
        Commit all pending batch write operations.

        Raises:
            PutError: If the maximum retry attempts are exceeded
        """

        async def _tasks() -> typing.AsyncIterator[
            typing.Coroutine[
                typing.Any,
                typing.Any,
                typing.Optional[typing.Dict[str, typing.Any]],
            ]
        ]:
            for chunk in ichunked(self.pending_operations, BATCH_WRITE_PAGE_LIMIT):
                put_items: typing.List[SerializedItem] = []
                delete_items: typing.List[SerializedItem] = []

                for item in chunk:
                    if item.get("action") == PUT:
                        put_items.append(item["item"].serialize())
                    elif item.get("action") == DELETE:
                        delete_items.append(item["item"]._get_keys())
                    elif PUT_REQUEST in item:
                        put_items.append(item[PUT_REQUEST]["ITEM"])
                    elif DELETE_REQUEST in item:
                        delete_items.append(item[DELETE_REQUEST]["KEY"])

                yield self.model._async_get_connection().batch_write_item(
                    put_items=put_items, delete_items=delete_items
                )
                await asyncio.sleep(0)
            self.pending_operations = []

        retries = 0
        unprocessed_batch_items = list(self.pending_operations)

        while unprocessed_batch_items and retries < self.model.Meta.max_retry_attempts:
            async for data in as_completed([i async for i in _tasks()]):
                if data is None:
                    continue
                unprocessed_items = data.get(UNPROCESSED_ITEMS, {}).get(
                    self.model.Meta.table_name, []
                )
                self.pending_operations.extend(unprocessed_items)
            retries += 1
            await asyncio.sleep(0)
        if self.pending_operations:
            raise PutError("Failed to batch write items: max_retry_attempts exceeded")
