import contextlib
import logging
import threading
import typing
from contextlib import AbstractAsyncContextManager
from contextvars import ContextVar

import aioboto3
import asyncio
import types_aiobotocore_dynamodb
from aiobotocore.config import AioConfig
from botocore.exceptions import ClientError, BotoCoreError

from pynamodb.connection.abstracts import AbstractConnection
from pynamodb.connection.base import BOTOCORE_EXCEPTIONS, MetaTable
from pynamodb.constants import (
    SERVICE_NAME,
    DELETE_ITEM,
    UPDATE_ITEM,
    ITEM,
    PUT_ITEM,
    TRANSACT_CONDITION_CHECK,
    TRANSACT_DELETE,
    TRANSACT_PUT,
    TRANSACT_UPDATE,
    TRANSACT_ITEMS,
    TRANSACT_WRITE_ITEMS,
    REQUEST_ITEMS,
    PUT_REQUEST,
    DELETE_REQUEST,
    BATCH_WRITE_ITEM,
    KEYS,
    EXPRESSION_ATTRIBUTE_NAMES,
    PROJECTION_EXPRESSION,
    CONSISTENT_READ,
    BATCH_GET_ITEM,
    GET_ITEM,
    TABLE_NAME,
    FILTER_EXPRESSION,
    INDEX_NAME,
    LIMIT,
    SEGMENT,
    TOTAL_SEGMENTS,
    EXPRESSION_ATTRIBUTE_VALUES,
    SCAN,
    KEY_CONDITION_EXPRESSION,
    SELECT_VALUES,
    SELECT,
    SCAN_INDEX_FORWARD,
    QUERY,
    KEY,
    DESCRIBE_TABLE,
    LIST_TABLES,
    UPDATE_TABLE,
    UPDATE_TIME_TO_LIVE,
    DELETE_TABLE,
    CREATE_TABLE,
    RETURN_CONSUMED_CAPACITY,
    TOTAL,
    CONSUMED_CAPACITY,
    CAPACITY_UNITS,
    TABLE_KEY,
)
from pynamodb.exceptions import (
    DeleteError,
    UpdateError,
    PutError,
    TransactWriteError,
    GetError,
    ScanError,
    TableError,
    QueryError,
    VerboseClientError,
    CancellationReason,
    TableDoesNotExist,
)
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.operand import Path
from pynamodb.expressions.projection import create_projection_expression
from pynamodb.expressions.update import Action

thread_local = threading.local()

ClientContext: ContextVar[contextlib.AsyncExitStack | None] = ContextVar(
    "ClientContext", default=None
)

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class AsyncPynamoDBContext(AbstractAsyncContextManager):
    def __init__(self):
        current_ctx = ClientContext.get()
        if current_ctx is not None:
            raise RuntimeError(
                "You're already inside an async context. Only a single `AsyncPynamoDB` context is allowed."
            )

        stack = contextlib.AsyncExitStack()
        self._token = ClientContext.set(stack)

    def __aenter__(self):
        stack = ClientContext.get()
        if not stack:
            raise RuntimeError(
                "You're not inside an async context. You must use `AsyncPynamoDB` to create a connection."
            )
        return stack.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        stack = ClientContext.get()
        if not stack:
            return
        await stack.__aexit__(exc_type, exc_val, exc_tb)
        ClientContext.reset(self._token)


class AsyncConnection(AbstractConnection[aioboto3.Session]):
    @staticmethod
    def session_factory() -> aioboto3.Session:
        return aioboto3.Session()

    async def _make_api_call(
        self, operation_name: str, operation_kwargs: typing.Dict
    ) -> typing.Dict:
        try:
            client = await self.get_client()
            return await client._make_api_call(operation_name, operation_kwargs)  # type: ignore
        except ClientError as e:
            resp_metadata = e.response.get("ResponseMetadata", {}).get(
                "HTTPHeaders", {}
            )
            cancellation_reasons = e.response.get("CancellationReasons", [])

            botocore_props = {"Error": e.response.get("Error", {})}
            verbose_props = {
                "request_id": resp_metadata.get("x-amzn-requestid", ""),
                "table_name": self._get_table_name_for_error_context(operation_kwargs),
            }
            raise VerboseClientError(
                botocore_props,
                operation_name,
                verbose_props,
                cancellation_reasons=(
                    (
                        CancellationReason(
                            code=d["Code"],
                            message=d.get("Message"),
                            raw_item=typing.cast(
                                typing.Optional[
                                    typing.Dict[str, typing.Dict[str, typing.Any]]
                                ],
                                d.get("Item"),
                            ),
                        )
                        if d["Code"] != "None"
                        else None
                    )
                    for d in cancellation_reasons
                ),
            ) from e

    async def dispatch(
        self, operation_name: str, operation_kwargs: typing.Dict
    ) -> typing.Dict:
        if operation_name not in [
            DESCRIBE_TABLE,
            LIST_TABLES,
            UPDATE_TABLE,
            UPDATE_TIME_TO_LIVE,
            DELETE_TABLE,
            CREATE_TABLE,
        ]:
            if RETURN_CONSUMED_CAPACITY not in operation_kwargs:
                operation_kwargs.update(self.get_consumed_capacity_map(TOTAL))
        log.debug("Calling %s with arguments %s", operation_name, operation_kwargs)
        # TODO: Implement somethin for `self.send_pre_boto_callback(operation_name, req_uuid, table_name)`
        data = await self._make_api_call(operation_name, operation_kwargs)
        # TODO: Implement somethin for `self.send_post_boto_callback(operation_name, req_uuid, table_name)`
        if data and CONSUMED_CAPACITY in data:
            capacity = data.get(CONSUMED_CAPACITY)
            if isinstance(capacity, dict) and CAPACITY_UNITS in capacity:
                capacity = capacity.get(CAPACITY_UNITS)
            log.debug(
                "%s %s consumed %s units",
                data.get(TABLE_NAME, ""),
                operation_name,
                capacity,
            )
        return data

    async def get_client(self) -> types_aiobotocore_dynamodb.DynamoDBClient:
        if not self._client or (
            self._client._request_signer
            and not self._client._request_signer._credentials
        ):
            config = AioConfig(
                parameter_validation=False,  # Disable unnecessary validation for performance
                connect_timeout=self._connect_timeout_seconds,
                read_timeout=self._read_timeout_seconds,
                max_pool_connections=self._max_pool_connections,
                retries={
                    "total_max_attempts": 1 + self._max_retry_attempts_exception,
                    "mode": "standard",
                },
            )
            stack = ClientContext.get()
            if stack is None:
                raise RuntimeError(
                    "You're not inside an async context. You must use `AsyncPynamoDB` to create a connection."
                )
            client = self.session.client(
                SERVICE_NAME, region_name=self.region, endpoint_url=self.host, config=config
            ) # type: ignore[call-overload]
            self._client = await stack.enter_async_context(client)
        return typing.cast(types_aiobotocore_dynamodb.DynamoDBClient, self._client)

    async def delete_item(
        self,
        table_name: str,
        hash_key: str,
        range_key: typing.Optional[str] = None,
        condition: typing.Optional[Condition] = None,
        return_values: typing.Optional[str] = None,
        return_consumed_capacity: typing.Optional[str] = None,
        return_item_collection_metrics: typing.Optional[str] = None,
    ) -> typing.Dict:
        """
        Performs the DeleteItem operation and returns the result
        """
        operation_kwargs = self.get_operation_kwargs(
            table_name,
            hash_key,
            range_key=range_key,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
        )
        try:
            return await self.dispatch(DELETE_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise DeleteError("Failed to delete item: {}".format(e), e)

    async def update_item(
        self,
        table_name: str,
        hash_key: str,
        range_key: typing.Optional[str] = None,
        actions: typing.Optional[typing.Sequence[Action]] = None,
        condition: typing.Optional[Condition] = None,
        return_consumed_capacity: typing.Optional[str] = None,
        return_item_collection_metrics: typing.Optional[str] = None,
        return_values: typing.Optional[str] = None,
    ) -> typing.Dict:
        """
        Performs the UpdateItem operation
        """
        if not actions:
            raise ValueError("'actions' cannot be empty")

        operation_kwargs = self.get_operation_kwargs(
            table_name=table_name,
            hash_key=hash_key,
            range_key=range_key,
            actions=actions,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
        )
        try:
            return await self.dispatch(UPDATE_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise UpdateError("Failed to update item: {}".format(e), e)

    async def put_item(
        self,
        table_name: str,
        hash_key: str,
        range_key: typing.Optional[str] = None,
        attributes: typing.Optional[typing.Any] = None,
        condition: typing.Optional[Condition] = None,
        return_values: typing.Optional[str] = None,
        return_consumed_capacity: typing.Optional[str] = None,
        return_item_collection_metrics: typing.Optional[str] = None,
    ) -> typing.Dict:
        """
        Performs the PutItem operation and returns the result
        """
        operation_kwargs = self.get_operation_kwargs(
            table_name=table_name,
            hash_key=hash_key,
            range_key=range_key,
            key=ITEM,
            attributes=attributes,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
        )
        try:
            return await self.dispatch(PUT_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise PutError("Failed to put item: {}".format(e), e)

    async def transact_write_items(
        self,
        condition_check_items: typing.Sequence[typing.Dict],
        delete_items: typing.Sequence[typing.Dict],
        put_items: typing.Sequence[typing.Dict],
        update_items: typing.Sequence[typing.Dict],
        client_request_token: typing.Optional[str] = None,
        return_consumed_capacity: typing.Optional[str] = None,
        return_item_collection_metrics: typing.Optional[str] = None,
    ) -> typing.Dict:
        """
        Performs the TransactWrite operation and returns the result
        """
        transact_items: typing.List[typing.Dict] = []
        transact_items.extend(
            {TRANSACT_CONDITION_CHECK: item} for item in condition_check_items
        )
        transact_items.extend({TRANSACT_DELETE: item} for item in delete_items)
        transact_items.extend({TRANSACT_PUT: item} for item in put_items)
        transact_items.extend({TRANSACT_UPDATE: item} for item in update_items)

        operation_kwargs = self._get_transact_operation_kwargs(
            client_request_token=client_request_token,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
        )
        operation_kwargs[TRANSACT_ITEMS] = transact_items

        try:
            return await self.dispatch(TRANSACT_WRITE_ITEMS, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TransactWriteError("Failed to write transaction items", e)

    async def transact_get_items(
        self,
        get_items: typing.Sequence[typing.Dict],
        return_consumed_capacity: typing.Optional[str] = None,
    ) -> typing.Dict:
        raise NotImplementedError(
            "TransactGet is not supported in AsyncConnection yet."
        )

    async def batch_write_item(
        self,
        table_name: str,
        put_items: typing.Optional[
            typing.Sequence[typing.Dict[str, typing.Any]]
        ] = None,
        delete_items: typing.Optional[
            typing.Sequence[typing.Dict[str, typing.Any]]
        ] = None,
        return_consumed_capacity: typing.Optional[str] = None,
        return_item_collection_metrics: typing.Optional[str] = None,
    ) -> typing.Dict[str, typing.Any]:
        """Execute a batch write operation.
        Args:
            table_name: Name of the table to write to
            put_items: Items to put into DynamoDB
            delete_items: Keys of items to delete from DynamoDB
            return_consumed_capacity: Whether to return consumed capacity info
            return_item_collection_metrics: Whether to return collection metrics
        Returns:
            DynamoDB response containing operation metadata
        Raises:
            ValueError: If neither put_items nor delete_items is specified
            PutError: If the batch write operation fails
        """
        if put_items is None and delete_items is None:
            raise ValueError("Either put_items or delete_items must be specified")

        operation_kwargs: typing.Dict[str, typing.Any] = {
            REQUEST_ITEMS: {table_name: []}
        }

        # Add consumed capacity settings if requested
        if return_consumed_capacity:
            operation_kwargs.update(
                self.get_consumed_capacity_map(return_consumed_capacity)
            )

        # Add collection metrics settings if requested
        if return_item_collection_metrics:
            operation_kwargs.update(
                self.get_item_collection_map(return_item_collection_metrics)
            )

        # Process put requests
        put_items_list = []
        if put_items:
            for item in put_items:
                put_items_list.append(
                    {
                        PUT_REQUEST: self.get_item_attribute_map(
                            table_name, item, pythonic_key=False
                        )
                    }
                )
                await asyncio.sleep(0)

        # Process delete requests
        delete_items_list = []
        if delete_items:
            for item in delete_items:
                delete_items_list.append(
                    {
                        DELETE_REQUEST: self.get_item_attribute_map(
                            table_name, item, item_key=KEY, pythonic_key=False
                        )
                    }
                )
                await asyncio.sleep(0)

        # Combine all requests
        operation_kwargs[REQUEST_ITEMS][table_name] = delete_items_list + put_items_list
        try:
            return await self.dispatch(BATCH_WRITE_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise PutError("Failed to batch write items: {}".format(e), e)

    async def batch_get_item(
        self,
        table_name: str,
        keys: typing.Sequence[str],
        consistent_read: typing.Optional[bool] = None,
        return_consumed_capacity: typing.Optional[str] = None,
        attributes_to_get: typing.Optional[typing.Any] = None,
    ) -> typing.Dict:
        """
        Performs the batch get item operation
        """
        operation_kwargs: typing.Dict[str, typing.Any] = {
            REQUEST_ITEMS: {table_name: {}}
        }

        args_map: typing.Dict[str, typing.Any] = {}
        name_placeholders: typing.Dict[str, str] = {}
        if consistent_read:
            args_map[CONSISTENT_READ] = consistent_read
        if return_consumed_capacity:
            operation_kwargs.update(
                self.get_consumed_capacity_map(return_consumed_capacity)
            )
        if attributes_to_get is not None:
            projection_expression = create_projection_expression(
                attributes_to_get, name_placeholders
            )
            args_map[PROJECTION_EXPRESSION] = projection_expression
        if name_placeholders:
            args_map[EXPRESSION_ATTRIBUTE_NAMES] = self._reverse_dict(name_placeholders)
        operation_kwargs[REQUEST_ITEMS][table_name].update(args_map)

        keys_map: typing.Dict[str, typing.List] = {KEYS: []}
        for key in keys:
            keys_map[KEYS].append(self.get_item_attribute_map(table_name, key)[ITEM])
        operation_kwargs[REQUEST_ITEMS][table_name].update(keys_map)
        try:
            return await self.dispatch(BATCH_GET_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise GetError("Failed to batch get items: {}".format(e), e)

    async def get_item(
        self,
        table_name: str,
        hash_key: str,
        range_key: typing.Optional[str] = None,
        consistent_read: bool = False,
        attributes_to_get: typing.Optional[typing.Any] = None,
    ) -> typing.Dict:
        """
        Performs the GetItem operation and returns the result
        """
        operation_kwargs = self.get_operation_kwargs(
            table_name=table_name,
            hash_key=hash_key,
            range_key=range_key,
            consistent_read=consistent_read,
            attributes_to_get=attributes_to_get,
        )
        try:
            return await self.dispatch(GET_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise GetError("Failed to get item: {}".format(e), e)

    async def scan(
        self,
        table_name: str,
        filter_condition: typing.Optional[typing.Any] = None,
        attributes_to_get: typing.Optional[typing.Any] = None,
        limit: typing.Optional[int] = None,
        return_consumed_capacity: typing.Optional[str] = None,
        exclusive_start_key: typing.Optional[str] = None,
        segment: typing.Optional[int] = None,
        total_segments: typing.Optional[int] = None,
        consistent_read: typing.Optional[bool] = None,
        index_name: typing.Optional[str] = None,
    ) -> typing.Dict:
        """
        Performs the scan operation
        """
        self._check_condition("filter_condition", filter_condition)

        operation_kwargs: typing.Dict[str, typing.Any] = {TABLE_NAME: table_name}
        name_placeholders: typing.Dict[str, str] = {}
        expression_attribute_values: typing.Dict[str, typing.Any] = {}

        if filter_condition is not None:
            filter_expression = filter_condition.serialize(
                name_placeholders, expression_attribute_values
            )
            operation_kwargs[FILTER_EXPRESSION] = filter_expression
        if attributes_to_get is not None:
            projection_expression = create_projection_expression(
                attributes_to_get, name_placeholders
            )
            operation_kwargs[PROJECTION_EXPRESSION] = projection_expression
        if index_name:
            operation_kwargs[INDEX_NAME] = index_name
        if limit is not None:
            operation_kwargs[LIMIT] = limit
        if return_consumed_capacity:
            operation_kwargs.update(
                self.get_consumed_capacity_map(return_consumed_capacity)
            )
        if exclusive_start_key:
            operation_kwargs.update(
                self.get_exclusive_start_key_map(table_name, exclusive_start_key)
            )
        if segment is not None:
            operation_kwargs[SEGMENT] = segment
        if total_segments:
            operation_kwargs[TOTAL_SEGMENTS] = total_segments
        if consistent_read:
            operation_kwargs[CONSISTENT_READ] = consistent_read
        if name_placeholders:
            operation_kwargs[EXPRESSION_ATTRIBUTE_NAMES] = self._reverse_dict(
                name_placeholders
            )
        if expression_attribute_values:
            operation_kwargs[EXPRESSION_ATTRIBUTE_VALUES] = expression_attribute_values

        try:
            return await self.dispatch(SCAN, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise ScanError("Failed to scan table: {}".format(e), e)

    async def query(
        self,
        table_name: str,
        hash_key: str,
        range_key_condition: typing.Optional[Condition] = None,
        filter_condition: typing.Optional[typing.Any] = None,
        attributes_to_get: typing.Optional[typing.Any] = None,
        consistent_read: bool = False,
        exclusive_start_key: typing.Optional[typing.Any] = None,
        index_name: typing.Optional[str] = None,
        limit: typing.Optional[int] = None,
        return_consumed_capacity: typing.Optional[str] = None,
        scan_index_forward: typing.Optional[bool] = None,
        select: typing.Optional[str] = None,
    ) -> typing.Dict:
        """
        Performs the Query operation and returns the result
        """
        self._check_condition("range_key_condition", range_key_condition)
        self._check_condition("filter_condition", filter_condition)

        operation_kwargs: typing.Dict[str, typing.Any] = {TABLE_NAME: table_name}
        name_placeholders: typing.Dict[str, str] = {}
        expression_attribute_values: typing.Dict[str, typing.Any] = {}

        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table: {}".format(table_name))
        if index_name:
            if not tbl.has_index_name(index_name):
                raise ValueError(
                    "Table {} has no index: {}".format(table_name, index_name)
                )
            hash_keyname = tbl.get_index_hash_keyname(index_name)
        else:
            hash_keyname = tbl.hash_keyname

        hash_condition_value = {
            self.get_attribute_type(
                table_name, hash_keyname, hash_key
            ): self.parse_attribute(hash_key)
        }
        key_condition = Path([hash_keyname]) == hash_condition_value
        if range_key_condition is not None:
            key_condition &= range_key_condition

        operation_kwargs[KEY_CONDITION_EXPRESSION] = key_condition.serialize(
            name_placeholders, expression_attribute_values
        )
        if filter_condition is not None:
            filter_expression = filter_condition.serialize(
                name_placeholders, expression_attribute_values
            )
            operation_kwargs[FILTER_EXPRESSION] = filter_expression
        if attributes_to_get:
            projection_expression = create_projection_expression(
                attributes_to_get, name_placeholders
            )
            operation_kwargs[PROJECTION_EXPRESSION] = projection_expression
        if consistent_read:
            operation_kwargs[CONSISTENT_READ] = True
        if exclusive_start_key:
            operation_kwargs.update(
                self.get_exclusive_start_key_map(table_name, exclusive_start_key)
            )
        if index_name:
            operation_kwargs[INDEX_NAME] = index_name
        if limit is not None:
            operation_kwargs[LIMIT] = limit
        if return_consumed_capacity:
            operation_kwargs.update(
                self.get_consumed_capacity_map(return_consumed_capacity)
            )
        if select:
            if select.upper() not in SELECT_VALUES:
                raise ValueError("{} must be one of {}".format(SELECT, SELECT_VALUES))
            operation_kwargs[SELECT] = str(select).upper()
        if scan_index_forward is not None:
            operation_kwargs[SCAN_INDEX_FORWARD] = scan_index_forward
        if name_placeholders:
            operation_kwargs[EXPRESSION_ATTRIBUTE_NAMES] = self._reverse_dict(
                name_placeholders
            )
        if expression_attribute_values:
            operation_kwargs[EXPRESSION_ATTRIBUTE_VALUES] = expression_attribute_values

        try:
            return await self.dispatch(QUERY, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise QueryError("Failed to query items: {}".format(e), e)

    async def describe_table(self, table_name: str) -> typing.Dict:
        """
        Performs the DescribeTable operation
        """
        operation_kwargs = {TABLE_NAME: table_name}
        try:
            data = await self.dispatch(DESCRIBE_TABLE, operation_kwargs)
            table_data = data.get(TABLE_KEY)
            # For compatibility with existing code which uses Connection directly,
            # we can let DescribeTable set the meta table.
            if table_data:
                meta_table = MetaTable(table_data)
                if meta_table.table_name not in self._tables:
                    self.add_meta_table(meta_table)
            return typing.cast(typing.Dict, table_data)
        except BotoCoreError as e:
            raise TableError("Unable to describe table: {}".format(e), e)
        except ClientError as e:
            if "ResourceNotFound" in e.response["Error"]["Code"]:
                raise TableDoesNotExist(e.response["Error"]["Message"])
            else:
                raise
