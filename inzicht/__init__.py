from .aio.crud.factories import async_session_factory
from .aio.crud.generic import AioGenericCRUD
from .crud.factories import session_factory
from .crud.generic import GenericCRUD
from .declarative import DeclarativeBase, InzichtBase

try:
    from sqlmodel import SQLModel as OriginalSQLModel  # type: ignore[import-not-found]

    class SQLModel(OriginalSQLModel, InzichtBase):
        __mapper_args__ = {"eager_defaults": True}
except ImportError:
    pass


__all__ = [
    "DeclarativeBase",
    "GenericCRUD",
    "session_factory",
    "AioGenericCRUD",
    "async_session_factory",
    "SQLModel",
]
