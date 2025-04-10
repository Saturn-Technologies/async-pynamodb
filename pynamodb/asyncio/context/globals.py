from contextvars import ContextVar

import aioboto3
import types_aiobotocore_dynamodb
from aiobotocore.config import AioConfig

from pynamodb.constants import SERVICE_NAME
from pynamodb.settings import get_settings_value
from contextlib import asynccontextmanager

GlobalPynamoDBClient = ContextVar[types_aiobotocore_dynamodb.DynamoDBClient | None]("GlobalClient", default=None)

@asynccontextmanager
async def create_global_client(session: aioboto3.Session, region: str, host: str | None = None, connection_timeout_seconds: int | None = None, read_timeout_seconds: int | None = None, max_pool_connections: int | None = None, max_retry_attempts: int | None = None):
    """Instantiates a global client for all async pynamodb calls..

    The ``create_global_client`` context manager is an experimental feature that allows you to reuse a single DynamoDB client across all PynamoDB calls in your application.
    This can improve performance by reducing the number of client connections, but comes with some limitations.

    .. warning::
        This is an experimental feature. The API may change in future releases.

    Basic Usage
    ----------

    Here's a basic example of how to use the global client:

    .. code-block:: python

        from pynamodb.asyncio.context.globals import create_global_client

        async def main():
            async with create_global_client(
                session=aioboto3.Session(),
                region="us-west-2",
                host="http://localhost:8000",
                connection_timeout_seconds=30,
                read_timeout_seconds=30
            ):
                # All PynamoDB operations within this context will use the same client
                await UserModel.get("user123")
                await OrderModel.scan()

    API Server Example
    ----------------

    When using PynamoDB in an API server, you typically want to initialize the client during startup and clean it up during shutdown:

    .. code-block:: python

        from fastapi import FastAPI
        from pynamodb.asyncio.context.globals import create_global_client

        app = FastAPI()
        client_context = None

        @app.on_event("startup")
        async def startup():
            global client_context
            client_context = create_global_client(
                session=aioboto3.Session(),
                region="us-west-2",
                host=None,  # Use AWS DynamoDB
                max_pool_connections=50
            )
            await client_context.__aenter__()

        @app.on_event("shutdown")
        async def shutdown():
            if client_context:
                await client_context.__aexit__(None, None, None)

    Limitations
    ----------

    1. Table Configuration Override
        The global client will ignore individual table configurations for:
        - Region
        - Host
        - Connection timeouts
        - Read timeouts
        - Max pool connections
        - Retry attempts

        Instead, it uses the configuration provided when creating the global client.

    2. AsyncPynamoDBContext
        Using ``create_global_client`` will effectively override the behavior of ``AsyncPynamoDBContext`` in most cases, as it takes precedence in client management.

    Configuration Parameters
    ----------------------

    - ``session``: An ``aioboto3.Session`` instance
    - ``region``: AWS region (e.g., "us-west-2")
    - ``host``: Optional DynamoDB endpoint URL
    - ``connection_timeout_seconds``: Connection timeout in seconds
    - ``read_timeout_seconds``: Read timeout in seconds
    - ``max_pool_connections``: Maximum number of connections in the connection pool
    - ``max_retry_attempts``: Maximum number of retry attempts for failed operations
    """
    config = AioConfig(
        parameter_validation=False,  # Disable unnecessary validation for performance
        connect_timeout=connection_timeout_seconds or get_settings_value(
            "connect_timeout_seconds"
        ),
        read_timeout=read_timeout_seconds or get_settings_value("read_timeout_seconds"),
        max_pool_connections=max_pool_connections or get_settings_value(
            "max_pool_connections"
        ),
        retries={
            "total_max_attempts": 1 + (max_retry_attempts or get_settings_value("max_retry_attempts")),
            "mode": "standard",
        },
    )
    token = None
    try:
        async with session.client(
            SERVICE_NAME,
            region_name=region,
            endpoint_url=host,
            config=config,
        ) as client: # type: ignore[call-overload]
            token = GlobalPynamoDBClient.set(client)
            yield client
    finally:
        if token is not None:
            GlobalPynamoDBClient.reset(token)
