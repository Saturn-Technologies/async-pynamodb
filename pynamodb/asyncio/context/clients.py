import asyncio
from contextvars import ContextVar, Token

import aioboto3
import types_aiobotocore_dynamodb
from aiobotocore.config import AioConfig

from pynamodb.asyncio.context.stack import add_to_stack
from pynamodb.constants import SERVICE_NAME

_ClientsContext: ContextVar[dict | None] = ContextVar(
    "_ClientsContext", default=None
)

def _get_client(connection_id: str) -> types_aiobotocore_dynamodb.DynamoDBClient | None:
    clients = _ClientsContext.get()
    if clients is None:
        return None
    return clients.get(connection_id)

async def _create_client(
    session: aioboto3.Session,
    connection_id: str,
    region: str,
    host: str | None,
    config: AioConfig,
) -> types_aiobotocore_dynamodb.DynamoDBClient:
    clients = _ClientsContext.get()
    if clients is None:
        raise RuntimeError("Can't create a client outside an async context")
    # Check if the client is already in the stack
    if connection_id in clients:
        await asyncio.sleep(0)
        return clients[connection_id]

    # Create the client
    _client_cm = session.client(
        SERVICE_NAME,
        region_name=region,
        endpoint_url=host,
        config=config,
    ) # type: ignore[call-overload]
    client = await add_to_stack(_client_cm)

    # Add the client to the stack
    clients[connection_id] = client
    _ClientsContext.set(clients)
    return client

async def get_or_create_client(
    *,
    session: aioboto3.Session,
    connection_id: str,
    region: str,
    host: str | None,
    config: AioConfig,
) -> types_aiobotocore_dynamodb.DynamoDBClient:
    client = _get_client(connection_id)
    if client is not None:
        await asyncio.sleep(0)
        return client
    return await _create_client(session, connection_id, region, host, config)

def create_client_stack() -> Token[dict | None]:
    clients = _ClientsContext.get()
    if clients is not None:
        raise RuntimeError("Trying to create a client stack while one already exists")
    clients = {}
    return _ClientsContext.set(clients)

def reset_client_stack(token: Token[dict | None]) -> None:
    _ClientsContext.reset(token)