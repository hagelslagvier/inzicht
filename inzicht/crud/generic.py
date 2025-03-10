from collections.abc import Generator, Sequence
from typing import Any, TypeVar

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from inzicht.crud.errors import DoesNotExistError
from inzicht.crud.interfaces import CRUDInterface
from inzicht.declarative import DeclarativeBase

T = TypeVar("T", bound=DeclarativeBase)


class GenericCRUD(CRUDInterface[T]):
    def __init__(self, session: Session) -> None:
        self.session = session

    @classmethod
    def get_model(cls) -> type[T]:
        (bases,) = cls.__orig_bases__  # type: ignore  # noqa
        (model,) = bases.__args__
        return model

    def count(self, where: Any | None = None) -> int:
        model = self.get_model()
        query = select(func.count()).select_from(model)
        if where is not None:
            query = query.filter(where)
        count = self.session.execute(query).scalar() or 0
        return count

    def create(self, **kwargs: Any) -> T:
        model = self.get_model()
        instance = model.new(**kwargs)
        self.session.add(instance)
        self.session.flush()
        return instance

    def bulk_create(self, instances: Sequence[T]) -> Sequence[T]:
        self.session.add_all(instances)
        self.session.flush()
        return instances

    def get(self, id: int | str, /) -> T:
        model = self.get_model()
        instance = self.session.get(model, id)
        if not instance:
            raise DoesNotExistError(
                f"Instance of model='{model}' with id='{id}' was not found"
            )
        return instance

    def read(
        self,
        *,
        where: Any | None = None,
        order_by: Any | None = None,
        skip: int = 0,
        take: int = 10,
    ) -> Generator[T, None, None]:
        model = self.get_model()
        query = select(model)
        if where is not None:
            query = query.filter(where)
        if order_by is not None:
            query = query.order_by(order_by)
        if skip:
            query = query.offset(skip)
        if take:
            query = query.limit(take)
        items = (item for item in self.session.execute(query).scalars())
        return items

    def update(self, id: int | str, /, **kwargs: Any) -> T:
        model = self.get_model()
        instance = self.session.get(model, id, with_for_update={"nowait": True})
        if not instance:
            raise DoesNotExistError(
                f"Instance of model='{model}' with id='{id}' was not found"
            )
        instance.update(**kwargs)
        self.session.add(instance)
        self.session.flush()
        return instance

    def delete(self, id: int | str, /) -> T:
        instance = self.get(id)
        self.session.delete(instance)
        self.session.flush()
        return instance
