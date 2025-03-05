import asyncio
import contextlib
from contextlib import AsyncExitStack
from contextvars import ContextVar, Token
from typing import cast, TypeVar, Any, AsyncContextManager

_StackContext: ContextVar[contextlib.AsyncExitStack | None] = ContextVar(
    "_StackContext", default=None
)
_lock = asyncio.Lock()

# Type variable for the return type of __aenter__
_T = TypeVar("_T")

async def create_stack(fail_if_exists: bool = False) -> tuple[AsyncExitStack, Token[AsyncExitStack | None]]:
    async with _lock:
        if fail_if_exists and _StackContext.get() is not None:
            raise RuntimeError(
                "You're already inside an async context. Only a single `AsyncPynamoDB` context is allowed."
            )
        stack = contextlib.AsyncExitStack()
        token = _StackContext.set(stack)
        return cast(contextlib.AsyncExitStack, _StackContext.get()), token

async def add_to_stack(cm: AsyncContextManager[_T]) -> _T:
    """
    Add an async context manager to the current exit stack.
    
    This function retrieves the current AsyncExitStack from context and adds
    the provided async context manager to it using enter_async_context.
    
    Args:
        cm: The async context manager to add to the stack
        
    Returns:
        The value returned by the async context manager's __aenter__ method
        
    Raises:
        RuntimeError: If called outside of a valid stack context
    """
    async with _lock:
        stack = _StackContext.get()
        if stack is None:
            raise RuntimeError(
                "No active async context stack found. Call create_stack() first."
            )
        # Add the context manager to the stack and return the result of __aenter__
        return await stack.enter_async_context(cm)

def get_stack() -> AsyncExitStack | None:
    return _StackContext.get()

async def reset_stack(token: Token[AsyncExitStack | None]) -> None:
    async with _lock:
        _StackContext.reset(token)