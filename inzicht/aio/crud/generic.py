from asyncio import Lock
from collections.abc import Generator, Sequence
from typing import Any, TypeVar

import sqlalchemy.exc
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from inzicht.aio.crud.interfaces import AioCRUDInterface
from inzicht.crud.errors import DoesNotExistError, IntegrityError, UnknowError
from inzicht.declarative import DeclarativeBase

T = TypeVar("T", bound=DeclarativeBase)


class AioGenericCRUD(AioCRUDInterface[T]):
    def __init__(self, async_session: AsyncSession) -> None:
        self.async_session = async_session
        self.lock = Lock()

    @classmethod
    def get_model(cls) -> type[T]:
        (bases,) = cls.__orig_bases__  # type: ignore  # noqa
        (model,) = bases.__args__
        return model

    async def count(self, where: Any | None = None) -> int:
        model = self.get_model()
        query = select(func.count()).select_from(model)
        if where is not None:
            query = query.filter(where)
        result = await self.async_session.execute(query)
        count = result.scalar() or 0
        return count

    async def create(self, **kwargs: Any) -> T:
        model = self.get_model()
        instance = model.new(**kwargs)
        message = f"DB operation [CREATE] on instance '{instance}' of model '{model}'"
        try:
            async with self.lock:
                self.async_session.add(instance)
                await self.async_session.flush()
        except sqlalchemy.exc.IntegrityError as error:
            error_message = f"{message} failed because of integrity constraints"
            raise IntegrityError(error_message, **kwargs) from error
        except Exception as error:
            error_message = f"{message} failed because of unknown error"
            raise UnknowError(error_message, **kwargs) from error
        return instance

    async def bulk_create(self, instances: Sequence[T]) -> Sequence[T]:
        model = self.get_model()
        message = f"DB operation [BULK_CREATE] on {len(instances)} instances '[{instances[0]},...]' of model '{model}'"
        try:
            self.async_session.add_all(instances)
            await self.async_session.flush()
        except sqlalchemy.exc.IntegrityError as error:
            error_message = f"{message} failed because of integrity constraints"
            raise IntegrityError(error_message) from error
        except Exception as error:
            error_message = f"{message} failed because of unknow error"
            raise UnknowError(error_message) from error
        return instances

    async def get(self, id: int | str, /) -> T:
        model = self.get_model()
        instance = await self.async_session.get(model, id)
        message = f"DB operation [GET] on instance of model '{model}' with id '{id}'"
        if not instance:
            error_message = f"{message} failed because the instance was not found"
            raise DoesNotExistError(error_message, id=id)
        return instance

    async def read(
        self,
        *,
        where: Any | None = None,
        order_by: Any | None = None,
        skip: int = 0,
        take: int | None = None,
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
        result = await self.async_session.execute(query)
        items = (item for item in result.scalars())
        return items

    async def update(self, id: int | str, /, **kwargs: Any) -> T:
        model = self.get_model()
        instance = await self.async_session.get(
            model, id, with_for_update={"nowait": True}
        )
        message = f"DB operation [UPDATE] on instance of model '{model}' with id '{id}'"
        if not instance:
            error_message = f"{message} failed because the instance was not found"
            raise DoesNotExistError(error_message, id=id, **kwargs)
        instance.update(**kwargs)
        try:
            self.async_session.add(instance)
            await self.async_session.flush()
        except sqlalchemy.exc.IntegrityError as error:
            error_message = f"{message} failed because of integrity constraints"
            raise IntegrityError(error_message, **kwargs) from error
        except Exception as error:
            error_message = f"{message} failed because of unknow error"
            raise UnknowError(error_message, **kwargs) from error
        return instance

    async def delete(self, id: int | str, /) -> T:
        model = self.get_model()
        instance = await self.get(id)
        message = f"DB operation [DELETE] on instance {instance} of model '{model}' with id '{id}'"
        if not instance:
            error_message = f"{message} failed because the instance was not found"
            raise DoesNotExistError(error_message)
        await self.async_session.delete(instance)
        await self.async_session.flush()
        return instance
