from contextlib import contextmanager
from typing import Any, Generator

from ddtrace import tracer
from ddtrace.ext import SpanTypes

from pynamodb.constants import TABLE_NAME


@contextmanager
def async_tracer(operation_name: str, operation_kwargs: dict) -> Generator[None, Any, None]:
    with tracer.trace("pynamodb.async_command", service="pynamodb", span_type=SpanTypes.HTTP, resource=operation_name) as span:
        if operation_kwargs.get(TABLE_NAME, None):
            table_name = operation_kwargs[TABLE_NAME]
            span.set_tag("table_name", table_name)
            span.resource = span.resource + " " + table_name
        meta = {
            "aws.agent": "pynamodb",
            "aws.operation": operation_name,
            "asyncio": True
        }
        span.set_tags(meta)
        yield