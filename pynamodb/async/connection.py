import contextlib
import threading
import typing
from contextlib import AbstractAsyncContextManager
from contextvars import ContextVar

import aioboto3
import types_aiobotocore_dynamodb
from aiobotocore.config import AioConfig

from pynamodb.connection.abstracts import AbstractConnection
from pynamodb.constants import SERVICE_NAME

thread_local = threading.local()

ClientContext: ContextVar[contextlib.AsyncExitStack | None] = ContextVar(
    "ClientContext", default=None
)


class AsyncPynamoDB(AbstractAsyncContextManager):
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
        return stack.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        stack = ClientContext.get()
        await stack.__aexit__(exc_type, exc_val, exc_tb)
        ClientContext.reset(self._token)


class AsyncConnection(AbstractConnection[aioboto3.Session]):
    SESSION_FACTORY = aioboto3.Session

    async def dispatch(
        self, operation_name: str, operation_kwargs: typing.Dict
    ) -> typing.Dict:
        raise NotImplementedError()

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
                SERVICE_NAME, self.region, endpoint_url=self.host, config=config
            )
            self._client = await stack.enter_async_context(client)
        return self._client
