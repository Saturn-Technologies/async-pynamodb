import abc
from abc import abstractmethod
from threading import local
from typing import Any, Dict, Mapping, Optional, Sequence

from botocore.session import get_session
from typing_extensions import overload


from pynamodb.connection._botocore_private import BotocoreBaseClientPrivate
from pynamodb.constants import (
    RETURN_CONSUMED_CAPACITY_VALUES,
    RETURN_ITEM_COLL_METRICS_VALUES,
    RETURN_ITEM_COLL_METRICS,
    RETURN_CONSUMED_CAPACITY,
    RETURN_VALUES_VALUES,
    CONSISTENT_READ,
    RETURN_VALUES,
    PROJECTION_EXPRESSION,
    TABLE_NAME,
    ITEM,
    KEY,
    ATTRIBUTE_TYPES,
    EXPRESSION_ATTRIBUTE_NAMES,
    EXPRESSION_ATTRIBUTE_VALUES,
    CONDITION_EXPRESSION,
    CLIENT_REQUEST_TOKEN,
    UPDATE_EXPRESSION,
    RETURN_VALUES_ON_CONDITION_FAILURE_VALUES,
    RETURN_VALUES_ON_CONDITION_FAILURE, REQUEST_ITEMS, TRANSACT_ITEMS,
)
from pynamodb.exceptions import (
    TableError,
)
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.projection import create_projection_expression
from pynamodb.expressions.update import Action, Update
from pynamodb.settings import get_settings_value

import typing

if typing.TYPE_CHECKING:
    from pynamodb.connection.base import MetaTable
else:
    MetaTable = object

T_BotoSession = typing.TypeVar("T_BotoSession")

class AbstractConnection(abc.ABC, typing.Generic[T_BotoSession]):
    def __init__(
        self,
        region: Optional[str] = None,
        host: Optional[str] = None,
        read_timeout_seconds: Optional[float] = None,
        connect_timeout_seconds: Optional[float] = None,
        max_retry_attempts: Optional[int] = None,
        max_pool_connections: Optional[int] = None,
        extra_headers: Optional[Mapping[str, str]] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        self._tables: Dict[str, "MetaTable"] = {}
        self.host = host
        self._local = local()
        self._client: Optional[BotocoreBaseClientPrivate] = None
        self._convert_to_request_dict__endpoint_url = False
        if region:
            self.region = region
        else:
            self.region = get_settings_value("region")

        if connect_timeout_seconds is not None:
            self._connect_timeout_seconds = connect_timeout_seconds
        else:
            self._connect_timeout_seconds = get_settings_value(
                "connect_timeout_seconds"
            )

        if read_timeout_seconds is not None:
            self._read_timeout_seconds = read_timeout_seconds
        else:
            self._read_timeout_seconds = get_settings_value("read_timeout_seconds")

        if max_retry_attempts is not None:
            self._max_retry_attempts_exception = max_retry_attempts
        else:
            self._max_retry_attempts_exception = get_settings_value(
                "max_retry_attempts"
            )

        if max_pool_connections is not None:
            self._max_pool_connections = max_pool_connections
        else:
            self._max_pool_connections = get_settings_value("max_pool_connections")

        if extra_headers is not None:
            self._extra_headers = extra_headers
        else:
            self._extra_headers = get_settings_value("extra_headers")

        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_session_token = aws_session_token

    def __repr__(self) -> str:
        return "Connection<{}>".format(self.client.meta.endpoint_url)

    @staticmethod
    def session_factory() -> T_BotoSession:
        return get_session()

    @property
    def session(self) -> T_BotoSession:
        """
        Returns a valid botocore session
        """
        # botocore client creation is not thread safe as of v1.2.5+ (see issue #153)
        if getattr(self._local, 'session', None) is None:
            self._local.session = self.session_factory()
            if self._aws_access_key_id and self._aws_secret_access_key:
                self._local.session.set_credentials(self._aws_access_key_id,
                                                        self._aws_secret_access_key,
                                                        self._aws_session_token)
        return self._local.session

    def add_meta_table(self, meta_table: MetaTable) -> None:
        """
        Adds information about the table's schema.
        """
        if meta_table.table_name in self._tables:
            raise ValueError(f"Meta-table for '{meta_table.table_name}' already added")
        self._tables[meta_table.table_name] = meta_table

    def get_meta_table(self, table_name: str) -> MetaTable:
        """
        Returns information about the table's schema.
        """
        try:
            return self._tables[table_name]
        except KeyError:
            raise TableError(f"Meta-table for '{table_name}' not initialized") from None

    def get_item_attribute_map(
        self,
        table_name: str,
        attributes: Any,
        item_key: str = ITEM,
        pythonic_key: bool = True,
    ) -> Dict:
        """
        Builds up a dynamodb compatible AttributeValue map
        """
        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {}".format(table_name))
        return tbl.get_item_attribute_map(
            attributes, item_key=item_key, pythonic_key=pythonic_key
        )

    def parse_attribute(self, attribute: Any, return_type: bool = False) -> Any:
        """
        Returns the attribute value, where the attribute can be
        a raw attribute value, or a dictionary containing the type:
        {'S': 'String value'}
        """
        if isinstance(attribute, dict):
            for key in ATTRIBUTE_TYPES:
                if key in attribute:
                    if return_type:
                        return key, attribute.get(key)
                    return attribute.get(key)
            raise ValueError("Invalid attribute supplied: {}".format(attribute))
        else:
            if return_type:
                return None, attribute
            return attribute

    def get_attribute_type(
        self, table_name: str, attribute_name: str, value: Optional[Any] = None
    ) -> str:
        """
        Returns the proper attribute type for a given attribute name
        :param value: The attribute value an be supplied just in case the type is already included
        """
        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {}".format(table_name))
        return tbl.get_attribute_type(attribute_name, value=value)

    def get_identifier_map(
        self,
        table_name: str,
        hash_key: str,
        range_key: Optional[str] = None,
        key: str = KEY,
    ) -> Dict:
        """
        Builds the identifier map that is common to several operations
        """
        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {}".format(table_name))
        return tbl.get_identifier_map(hash_key, range_key=range_key, key=key)

    def get_consumed_capacity_map(self, return_consumed_capacity: str) -> Dict:
        """
        Builds the consumed capacity map that is common to several operations
        """
        if return_consumed_capacity.upper() not in RETURN_CONSUMED_CAPACITY_VALUES:
            raise ValueError(
                "{} must be one of {}".format(
                    RETURN_ITEM_COLL_METRICS, RETURN_CONSUMED_CAPACITY_VALUES
                )
            )
        return {RETURN_CONSUMED_CAPACITY: str(return_consumed_capacity).upper()}

    def get_return_values_map(self, return_values: str) -> Dict:
        """
        Builds the return values map that is common to several operations
        """
        if return_values.upper() not in RETURN_VALUES_VALUES:
            raise ValueError(
                "{} must be one of {}".format(RETURN_VALUES, RETURN_VALUES_VALUES)
            )
        return {RETURN_VALUES: str(return_values).upper()}

    def get_return_values_on_condition_failure_map(
        self, return_values_on_condition_failure: str
    ) -> Dict:
        """
        Builds the return values map that is common to several operations
        """
        if return_values_on_condition_failure.upper() not in RETURN_VALUES_VALUES:
            raise ValueError(
                "{} must be one of {}".format(
                    RETURN_VALUES_ON_CONDITION_FAILURE,
                    RETURN_VALUES_ON_CONDITION_FAILURE_VALUES,
                )
            )
        return {
            RETURN_VALUES_ON_CONDITION_FAILURE: str(
                return_values_on_condition_failure
            ).upper()
        }

    def get_item_collection_map(self, return_item_collection_metrics: str) -> Dict:
        """
        Builds the item collection map
        """
        if (
            return_item_collection_metrics.upper()
            not in RETURN_ITEM_COLL_METRICS_VALUES
        ):
            raise ValueError(
                "{} must be one of {}".format(
                    RETURN_ITEM_COLL_METRICS, RETURN_ITEM_COLL_METRICS_VALUES
                )
            )
        return {RETURN_ITEM_COLL_METRICS: str(return_item_collection_metrics).upper()}

    def get_exclusive_start_key_map(
        self, table_name: str, exclusive_start_key: str
    ) -> Dict:
        """
        Builds the exclusive start key attribute map
        """
        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {}".format(table_name))
        return tbl.get_exclusive_start_key_map(exclusive_start_key)

    def get_operation_kwargs(
        self,
        table_name: str,
        hash_key: str,
        range_key: Optional[str] = None,
        key: str = KEY,
        attributes: Optional[Any] = None,
        attributes_to_get: Optional[Any] = None,
        actions: Optional[Sequence[Action]] = None,
        condition: Optional[Condition] = None,
        consistent_read: Optional[bool] = None,
        return_values: Optional[str] = None,
        return_consumed_capacity: Optional[str] = None,
        return_item_collection_metrics: Optional[str] = None,
        return_values_on_condition_failure: Optional[str] = None,
    ) -> Dict:
        self._check_condition("condition", condition)

        operation_kwargs: Dict[str, Any] = {}
        name_placeholders: Dict[str, str] = {}
        expression_attribute_values: Dict[str, Any] = {}

        operation_kwargs[TABLE_NAME] = table_name
        operation_kwargs.update(
            self.get_identifier_map(table_name, hash_key, range_key, key=key)
        )
        if attributes and operation_kwargs.get(ITEM) is not None:
            attrs = self.get_item_attribute_map(table_name, attributes)
            operation_kwargs[ITEM].update(attrs[ITEM])
        if attributes_to_get is not None:
            projection_expression = create_projection_expression(
                attributes_to_get, name_placeholders
            )
            operation_kwargs[PROJECTION_EXPRESSION] = projection_expression
        if condition is not None:
            condition_expression = condition.serialize(
                name_placeholders, expression_attribute_values
            )
            operation_kwargs[CONDITION_EXPRESSION] = condition_expression
        if consistent_read is not None:
            operation_kwargs[CONSISTENT_READ] = consistent_read
        if return_values is not None:
            operation_kwargs.update(self.get_return_values_map(return_values))
        if return_values_on_condition_failure is not None:
            operation_kwargs.update(
                self.get_return_values_on_condition_failure_map(
                    return_values_on_condition_failure
                )
            )
        if return_consumed_capacity is not None:
            operation_kwargs.update(
                self.get_consumed_capacity_map(return_consumed_capacity)
            )
        if return_item_collection_metrics is not None:
            operation_kwargs.update(
                self.get_item_collection_map(return_item_collection_metrics)
            )
        if actions is not None:
            update_expression = Update(*actions)
            operation_kwargs[UPDATE_EXPRESSION] = update_expression.serialize(
                name_placeholders, expression_attribute_values
            )
        if name_placeholders:
            operation_kwargs[EXPRESSION_ATTRIBUTE_NAMES] = self._reverse_dict(
                name_placeholders
            )
        if expression_attribute_values:
            operation_kwargs[EXPRESSION_ATTRIBUTE_VALUES] = expression_attribute_values
        return operation_kwargs

    def _get_transact_operation_kwargs(
        self,
        client_request_token: Optional[str] = None,
        return_consumed_capacity: Optional[str] = None,
        return_item_collection_metrics: Optional[str] = None,
    ) -> Dict:
        operation_kwargs = {}
        if client_request_token is not None:
            operation_kwargs[CLIENT_REQUEST_TOKEN] = client_request_token
        if return_consumed_capacity is not None:
            operation_kwargs.update(
                self.get_consumed_capacity_map(return_consumed_capacity)
            )
        if return_item_collection_metrics is not None:
            operation_kwargs.update(
                self.get_item_collection_map(return_item_collection_metrics)
            )

        return operation_kwargs

    def _check_condition(self, name, condition):
        if condition is not None:
            if not isinstance(condition, Condition):
                raise ValueError("'{}' must be an instance of Condition".format(name))

    @staticmethod
    def _reverse_dict(d):
        return {v: k for k, v in d.items()}

    def _get_table_name_for_error_context(self, operation_kwargs) -> str:
        # First handle the two multi-table cases: batch and transaction operations
        if REQUEST_ITEMS in operation_kwargs:
            return ','.join(operation_kwargs[REQUEST_ITEMS])
        elif TRANSACT_ITEMS in operation_kwargs:
            table_names = []
            for item in operation_kwargs[TRANSACT_ITEMS]:
                for op in item.values():
                    table_names.append(op[TABLE_NAME])
            return ",".join(table_names)
        return operation_kwargs.get(TABLE_NAME)

    @overload
    @abstractmethod
    async def dispatch(self, operation_name: str, operation_kwargs: Dict) -> Dict:
        ...

    @abstractmethod
    def dispatch(self, operation_name: str, operation_kwargs: Dict) -> Dict:
        ...


class AbstractTableConnection:
    CONNECTION_CLASS = AbstractConnection

    def __init__(
        self,
        table_name: str,
        region: Optional[str] = None,
        host: Optional[str] = None,
        connect_timeout_seconds: Optional[float] = None,
        read_timeout_seconds: Optional[float] = None,
        max_retry_attempts: Optional[int] = None,
        max_pool_connections: Optional[int] = None,
        extra_headers: Optional[Mapping[str, str]] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        *,
        meta_table: Optional[MetaTable] = None,
    ) -> None:
        self.table_name = table_name
        self.connection = self.CONNECTION_CLASS(
            region=region,
            host=host,
            connect_timeout_seconds=connect_timeout_seconds,
            read_timeout_seconds=read_timeout_seconds,
            max_retry_attempts=max_retry_attempts,
            max_pool_connections=max_pool_connections,
            extra_headers=extra_headers,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

        if meta_table is not None:
            self.connection.add_meta_table(meta_table)

    def get_meta_table(self) -> MetaTable:
        """
        Returns a MetaTable
        """
        return self.connection.get_meta_table(self.table_name)

    def get_operation_kwargs(
        self,
        hash_key: str,
        range_key: Optional[str] = None,
        key: str = KEY,
        attributes: Optional[Any] = None,
        attributes_to_get: Optional[Any] = None,
        actions: Optional[Sequence[Action]] = None,
        condition: Optional[Condition] = None,
        consistent_read: Optional[bool] = None,
        return_values: Optional[str] = None,
        return_consumed_capacity: Optional[str] = None,
        return_item_collection_metrics: Optional[str] = None,
        return_values_on_condition_failure: Optional[str] = None,
    ) -> Dict:
        return self.connection.get_operation_kwargs(
            self.table_name,
            hash_key,
            range_key=range_key,
            key=key,
            attributes=attributes,
            attributes_to_get=attributes_to_get,
            actions=actions,
            condition=condition,
            consistent_read=consistent_read,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
            return_values_on_condition_failure=return_values_on_condition_failure,
        )
