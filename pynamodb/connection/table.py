"""
PynamoDB Connection classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from typing import Any, Dict, Optional, Sequence

from pynamodb.connection import Connection
from pynamodb.connection.abstracts import AbstractTableConnection
from pynamodb.constants import DEFAULT_BILLING_MODE
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.update import Action


class TableConnection(AbstractTableConnection):
    """
    A higher level abstraction over botocore
    """

    CONNECTION_CLASS = Connection # type: ignore[assignment]


    def delete_item(
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
        return self.connection.delete_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
        )

    def update_item(
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
        return self.connection.update_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            actions=actions,
            condition=condition,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
            return_values=return_values,
        )

    def put_item(
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
        return self.connection.put_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            attributes=attributes,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
        )

    def batch_write_item(
        self,
        put_items: Optional[Any] = None,
        delete_items: Optional[Any] = None,
        return_consumed_capacity: Optional[str] = None,
        return_item_collection_metrics: Optional[str] = None,
    ) -> Dict:
        """
        Performs the batch_write_item operation
        """
        return self.connection.batch_write_item(
            self.table_name,
            put_items=put_items,
            delete_items=delete_items,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
        )

    def batch_get_item(
        self,
        keys: Sequence[str],
        consistent_read: Optional[bool] = None,
        return_consumed_capacity: Optional[str] = None,
        attributes_to_get: Optional[Any] = None,
    ) -> Dict:
        """
        Performs the batch get item operation
        """
        return self.connection.batch_get_item(
            self.table_name,
            keys,
            consistent_read=consistent_read,
            return_consumed_capacity=return_consumed_capacity,
            attributes_to_get=attributes_to_get,
        )

    def get_item(
        self,
        hash_key: str,
        range_key: Optional[str] = None,
        consistent_read: bool = False,
        attributes_to_get: Optional[Any] = None,
    ) -> Dict:
        """
        Performs the GetItem operation and returns the result
        """
        return self.connection.get_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            consistent_read=consistent_read,
            attributes_to_get=attributes_to_get,
        )

    def scan(
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
        return self.connection.scan(
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

    def query(
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
        return self.connection.query(
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

    def describe_table(self) -> Dict:
        """
        Performs the DescribeTable operation and returns the result
        """
        return self.connection.describe_table(self.table_name)

    def delete_table(self) -> Dict:
        """
        Performs the DeleteTable operation and returns the result
        """
        return self.connection.delete_table(self.table_name)

    def update_time_to_live(self, ttl_attr_name: str) -> Dict:
        """
        Performs the UpdateTimeToLive operation and returns the result
        """
        return self.connection.update_time_to_live(self.table_name, ttl_attr_name)

    def update_table(
        self,
        read_capacity_units: Optional[int] = None,
        write_capacity_units: Optional[int] = None,
        global_secondary_index_updates: Optional[Any] = None,
    ) -> Dict:
        """
        Performs the UpdateTable operation and returns the result
        """
        return self.connection.update_table(
            self.table_name,
            read_capacity_units=read_capacity_units,
            write_capacity_units=write_capacity_units,
            global_secondary_index_updates=global_secondary_index_updates)

    def create_table(
        self,
        attribute_definitions: Optional[Any] = None,
        key_schema: Optional[Any] = None,
        read_capacity_units: Optional[int] = None,
        write_capacity_units: Optional[int] = None,
        global_secondary_indexes: Optional[Any] = None,
        local_secondary_indexes: Optional[Any] = None,
        stream_specification: Optional[Dict] = None,
        billing_mode: str = DEFAULT_BILLING_MODE,
        tags: Optional[Dict[str, str]] = None,
    ) -> Dict:
        """
        Performs the CreateTable operation and returns the result
        """
        return self.connection.create_table(
            self.table_name,
            attribute_definitions=attribute_definitions,
            key_schema=key_schema,
            read_capacity_units=read_capacity_units,
            write_capacity_units=write_capacity_units,
            global_secondary_indexes=global_secondary_indexes,
            local_secondary_indexes=local_secondary_indexes,
            stream_specification=stream_specification,
            billing_mode=billing_mode,
            tags=tags
        )
