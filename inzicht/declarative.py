from typing import Any, ClassVar

from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase as OriginalBase
from sqlalchemy.orm.mapper import Mapper
from typing_extensions import Self


class InzichtBase:
    __mapper__: ClassVar[Mapper]

    @classmethod
    def _get_primary_key(cls) -> list[str]:
        primary_key = [c.name for c in cls.__mapper__.primary_key]
        return primary_key

    @classmethod
    def _get_columns(cls) -> list[str]:
        columns = [c.name for c in cls.__mapper__.columns]
        return columns

    @classmethod
    def _get_relationships(cls) -> list[str]:
        relationships = [r.key for r in cls.__mapper__.relationships]
        return relationships

    @classmethod
    def _get_attributes(cls) -> list[str]:
        primary_key = set(cls._get_primary_key())
        columns = set(cls._get_columns())
        relationships = set(cls._get_relationships())
        attributes = columns | relationships
        safe_attributes = list(attributes - primary_key)
        return safe_attributes

    @classmethod
    def new(cls, **kwargs: Any) -> Self:
        safe_kwargs = {k: v for k, v in kwargs.items() if k in cls._get_attributes()}
        return cls(**safe_kwargs)

    def update(self, **kwargs: Any) -> None:
        safe_kwargs = {k: v for k, v in kwargs.items() if k in self._get_attributes()}
        for k, v in safe_kwargs.items():
            setattr(self, k, v)


class DeclarativeBase(AsyncAttrs, OriginalBase, InzichtBase):
    __abstract__ = True
    __mapper_args__ = {"eager_defaults": True}
