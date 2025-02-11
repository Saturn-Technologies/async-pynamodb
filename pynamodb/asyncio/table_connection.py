from typing import Any, Dict, Optional, Sequence

from pynamodb.asyncio.connection import AsyncConnection
from pynamodb.connection.abstracts import AbstractTableConnection
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.update import Action


class AsyncTableConnection(AbstractTableConnection):
    """
    A higher level abstraction over botocore
    """

    connection: AsyncConnection
    CONNECTION_CLASS = AsyncConnection  # type: ignore[assignment]

    async def delete_item(
        self,
        hash_key: str,
        range_key: Optional[str] = None,
        condition: Optional[Condition] = None,
        return_values: Optional[str] = None,
        return_consumed_capacity: Optional[str] = None,
        return_item_collection_metrics: Optional[str] = None,
    ) -> Dict:
        """
        Performs the DeleteItem operation and returns the result
        """
        return await self.connection.delete_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
        )

    async def update_item(
        self,
        hash_key: str,
        range_key: Optional[str] = None,
        actions: Optional[Sequence[Action]] = None,
        condition: Optional[Condition] = None,
        return_consumed_capacity: Optional[str] = None,
        return_item_collection_metrics: Optional[str] = None,
        return_values: Optional[str] = None,
    ) -> Dict:
        """
        Performs the UpdateItem operation
        """
        return await self.connection.update_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            actions=actions,
            condition=condition,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
            return_values=return_values,
        )

    async def put_item(
        self,
        hash_key: str,
        range_key: Optional[str] = None,
        attributes: Optional[Any] = None,
        condition: Optional[Condition] = None,
        return_values: Optional[str] = None,
        return_consumed_capacity: Optional[str] = None,
        return_item_collection_metrics: Optional[str] = None,
    ) -> Dict:
        """
        Performs the PutItem operation and returns the result
        """
        return await self.connection.put_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            attributes=attributes,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
        )

    async def batch_write_item(
        self,
        put_items: Optional[Any] = None,
        delete_items: Optional[Any] = None,
        return_consumed_capacity: Optional[str] = None,
        return_item_collection_metrics: Optional[str] = None,
    ) -> Dict:
        """
        Performs the batch_write_item operation
        """
        return await self.connection.batch_write_item(
            self.table_name,
            put_items=put_items,
            delete_items=delete_items,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
        )

    async def batch_get_item(
        self,
        keys: Sequence[str],
        consistent_read: Optional[bool] = None,
        return_consumed_capacity: Optional[str] = None,
        attributes_to_get: Optional[Any] = None,
    ) -> Dict:
        """
        Performs the batch get item operation
        """
        return await self.connection.batch_get_item(
            self.table_name,
            keys,
            consistent_read=consistent_read,
            return_consumed_capacity=return_consumed_capacity,
            attributes_to_get=attributes_to_get,
        )

    async def get_item(
        self,
        hash_key: str,
        range_key: Optional[str] = None,
        consistent_read: bool = False,
        attributes_to_get: Optional[Any] = None,
    ) -> Dict:
        """
        Performs the GetItem operation and returns the result
        """
        return await self.connection.get_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            consistent_read=consistent_read,
            attributes_to_get=attributes_to_get,
        )

    async def scan(
        self,
        filter_condition: Optional[Any] = None,
        attributes_to_get: Optional[Any] = None,
        limit: Optional[int] = None,
        return_consumed_capacity: Optional[str] = None,
        segment: Optional[int] = None,
        total_segments: Optional[int] = None,
        exclusive_start_key: Optional[str] = None,
        consistent_read: Optional[bool] = None,
        index_name: Optional[str] = None,
    ) -> Dict:
        """
        Performs the scan operation
        """
        return await self.connection.scan(
            self.table_name,
            filter_condition=filter_condition,
            attributes_to_get=attributes_to_get,
            limit=limit,
            return_consumed_capacity=return_consumed_capacity,
            segment=segment,
            total_segments=total_segments,
            exclusive_start_key=exclusive_start_key,
            consistent_read=consistent_read,
            index_name=index_name,
        )

    async def query(
        self,
        hash_key: str,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Any] = None,
        attributes_to_get: Optional[Any] = None,
        consistent_read: bool = False,
        exclusive_start_key: Optional[Any] = None,
        index_name: Optional[str] = None,
        limit: Optional[int] = None,
        return_consumed_capacity: Optional[str] = None,
        scan_index_forward: Optional[bool] = None,
        select: Optional[str] = None,
    ) -> Dict:
        """
        Performs the Query operation and returns the result
        """
        return await self.connection.query(
            self.table_name,
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            attributes_to_get=attributes_to_get,
            consistent_read=consistent_read,
            exclusive_start_key=exclusive_start_key,
            index_name=index_name,
            limit=limit,
            return_consumed_capacity=return_consumed_capacity,
            scan_index_forward=scan_index_forward,
            select=select,
        )

    async def describe_table(self) -> Dict:
        """
        Performs the DescribeTable operation and returns the result
        """
        return await self.connection.describe_table(self.table_name)
