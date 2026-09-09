"""Allow audits to reuse parsers without their ingestion metadata writes."""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps

_writes_allowed = ContextVar('parser_metadata_writes_allowed', default=True)


def parser_writes_allowed():
    return _writes_allowed.get()


@contextmanager
def read_only_parser_scope():
    token = _writes_allowed.set(False)
    try:
        yield
    finally:
        _writes_allowed.reset(token)


def read_only_parsing(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        with read_only_parser_scope():
            return function(*args, **kwargs)
    return wrapped
