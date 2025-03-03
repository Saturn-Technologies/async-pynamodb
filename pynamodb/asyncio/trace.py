from typing import Any, Callable, Optional, TypeVar, ParamSpec

try:
    from ddtrace import tracer
except ImportError:
    tracer = None

F = TypeVar('F', bound=Callable[..., Any])
P = ParamSpec('P')


def pynamo_tracer(name: Optional[str] = None):
    """
    A decorator that traces an async function using ddtrace when available.
    If ddtrace is not available, the function is executed normally without tracing.
    
    Args:
        name: The name of the span. If not provided, the function name will be used.

    Returns:
        A decorator function that wraps the original async function.
    """
    def decorator(f: Callable[P, F]) -> Callable[P, F]:
        # If tracer is not available, just call the function
        if tracer is None:
            return f
        return tracer.wrap(name=name, service='pynamodb')
    return decorator