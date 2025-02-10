from __future__ import annotations

import typing
from contextlib import AbstractAsyncContextManager

from pynamodb.asyncio.connection import AsyncConnection
from pynamodb.transactions import TransactWrite

# Type definitions
T = typing.TypeVar("T")
TransactionResponse = typing.Dict[str, typing.Any]
TransactionItem = typing.Dict[str, typing.Any]

if typing.TYPE_CHECKING:
    from pynamodb.models import Model


class AsyncTransaction(AbstractAsyncContextManager):
    """
    Base class for async DynamoDB transactions.

    Args:
        return_consumed_capacity: Determines the level of detail about provisioned
            throughput consumption included in the response.
    """

    _async_connection: AsyncConnection

    def __init__(
        self,
        connection: AsyncConnection,
        return_consumed_capacity: typing.Optional[str] = None,
    ) -> None:
        self._return_consumed_capacity = return_consumed_capacity
        self._async_connection = connection

    async def _commit(self) -> None:
        """
        Commits the transaction. Must be implemented by subclasses.

        Raises:
            NotImplementedError: When called directly on base class
        """
        raise NotImplementedError

    async def __aenter__(self) -> AsyncTransaction:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: typing.Optional[type[BaseException]],
        exc_val: typing.Optional[BaseException],
        exc_tb: typing.Optional[typing.Any],
    ) -> None:
        """
        Exit the async context manager and commit if no exceptions occurred.

        Args:
            exc_type: The type of the exception that was raised
            exc_val: The instance of the exception that was raised
            exc_tb: The traceback of the exception that was raised
        """
        if all(x is None for x in (exc_type, exc_val, exc_tb)):
            await self._commit()


class AsyncTransactWrite(TransactWrite, AsyncTransaction):
    """
    Async implementation of DynamoDB TransactWrite operations.

    Args:
        client_request_token: Unique identifier for the transaction
        return_item_collection_metrics: Determines whether to return statistics about item collections
        **kwargs: Additional arguments passed to AsyncTransaction
    """

    def __init__(
        self,
        client_request_token: typing.Optional[str] = None,
        return_item_collection_metrics: typing.Optional[str] = None,
        **kwargs: typing.Any,
    ) -> None:
        AsyncTransaction.__init__(self, **kwargs)
        self._client_request_token: typing.Optional[str] = client_request_token
        self._return_item_collection_metrics = return_item_collection_metrics
        self._condition_check_items: typing.List[TransactionItem] = []
        self._delete_items: typing.List[TransactionItem] = []
        self._put_items: typing.List[TransactionItem] = []
        self._update_items: typing.List[TransactionItem] = []
        self._models_for_version_attribute_update: typing.List["Model"] = []

    async def _commit(self) -> TransactionResponse:
        """
        Commits the transaction by executing all queued operations.

        Returns:
            The response from DynamoDB containing transaction results

        Notes:
            After successful commit, version attributes of all affected
            models are updated locally.
        """
        response = await self._async_connection.transact_write_items(
            condition_check_items=self._condition_check_items,
            delete_items=self._delete_items,
            put_items=self._put_items,
            update_items=self._update_items,
            client_request_token=self._client_request_token,
            return_consumed_capacity=self._return_consumed_capacity,
            return_item_collection_metrics=self._return_item_collection_metrics,
        )

        # Update version attributes for all affected models
        for model in self._models_for_version_attribute_update:
            model.update_local_version_attribute()

        return response
