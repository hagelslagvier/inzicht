import logging
from asyncio import Lock
from collections.abc import Generator, Sequence
from typing import Any, TypeVar, get_args

import sqlalchemy.exc
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from inzicht.aio.crud.interfaces import AioCRUDInterface
from inzicht.crud.errors import DoesNotExistError, IntegrityError, UnknowError
from inzicht.declarative import DeclarativeBase

T = TypeVar("T", bound=DeclarativeBase)


formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)

logger = logging.getLogger("aio.crud.generic")
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class AioGenericCRUD(AioCRUDInterface[T]):
    def __init__(self, async_session: AsyncSession) -> None:
        self.async_session = async_session
        self.lock = Lock()

    def get_model(self) -> type[T]:
        if hasattr(self, "__orig_class__"):
            (model,) = get_args(self.__orig_class__)
        elif hasattr(self, "__orig_bases__"):
            (base,) = self.__orig_bases__
            (model,) = get_args(base)
        else:
            raise TypeError(
                f"Can't define type parameter of generic class {self.__class__}"
            )
        return model

    async def count(self, where: Any | None = None) -> int:
        model = self.get_model()
        query = select(func.count()).select_from(model)
        if where is not None:
            query = query.filter(where)
        result = await self.async_session.execute(query)
        count = result.scalar() or 0
        return count

    async def create(self, instance: T | None = None, /, **kwargs: Any) -> T:
        model = self.get_model()
        if instance and kwargs:
            raise ValueError(
                "Cannot provide both 'instance' and keyword arguments for creation"
            )
        instance = instance or model.new(**kwargs)
        header = f"DB operation [CREATE] on instance '{instance}' of model '{model}'"
        try:
            async with self.lock:
                self.async_session.add(instance)
                await self.async_session.flush()
        except sqlalchemy.exc.IntegrityError as error:
            error_message = f"{header} failed because of integrity constraints"
            logger.error(error_message)
            raise IntegrityError(error_message, **kwargs) from error
        except Exception as error:
            error_message = f"{header} failed because of unknown error"
            logger.error(error_message)
            raise UnknowError(error_message, **kwargs) from error
        logger.info(f"{header} succeeded")
        return instance

    async def bulk_create(self, instances: Sequence[T]) -> Sequence[T]:
        model = self.get_model()
        header = f"DB operation [BULK_CREATE] on {len(instances)} instances '[{instances[0]},...]' of model '{model}'"
        try:
            self.async_session.add_all(instances)
            await self.async_session.flush()
        except sqlalchemy.exc.IntegrityError as error:
            error_message = f"{header} failed because of integrity constraints"
            logger.error(error_message)
            raise IntegrityError(error_message) from error
        except Exception as error:
            error_message = f"{header} failed because of unknow error"
            logger.error(error_message)
            raise UnknowError(error_message) from error
        logger.info(f"{header} succeeded")
        return instances

    async def get(self, id: int | str, /) -> T:
        model = self.get_model()
        instance = await self.async_session.get(model, id)
        header = f"DB operation [GET] on instance of model '{model}' with id '{id}'"
        if not instance:
            error_message = f"{header} failed because the instance was not found"
            logger.error(error_message)
            raise DoesNotExistError(error_message, id=id)
        logger.info(f"{header} succeeded")
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
        header = f"DB operation [READ] on model '{model}'"
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
        logger.info(f"{header} succeeded")
        return items

    async def update(self, id: int | str, /, **kwargs: Any) -> T:
        model = self.get_model()
        instance = await self.async_session.get(
            model, id, with_for_update={"nowait": True}
        )
        header = f"DB operation [UPDATE] on instance of model '{model}' with id '{id}'"
        if not instance:
            error_message = f"{header} failed because the instance was not found"
            logger.error(error_message)
            raise DoesNotExistError(error_message, id=id, **kwargs)
        instance.update(**kwargs)
        try:
            self.async_session.add(instance)
            await self.async_session.flush()
        except sqlalchemy.exc.IntegrityError as error:
            error_message = f"{header} failed because of integrity constraints"
            logger.error(error_message)
            raise IntegrityError(error_message, **kwargs) from error
        except Exception as error:
            error_message = f"{header} failed because of unknow error"
            logger.error(error_message)
            raise UnknowError(error_message, **kwargs) from error
        logger.info(f"{header} succeeded")
        return instance

    async def delete(self, id: int | str, /) -> T:
        model = self.get_model()
        instance = await self.get(id)
        header = f"DB operation [DELETE] on instance {instance} of model '{model}' with id '{id}'"
        if not instance:
            error_message = f"{header} failed because the instance was not found"
            logger.error(error_message)
            raise DoesNotExistError(error_message)
        await self.async_session.delete(instance)
        await self.async_session.flush()
        logger.info(f"{header} succeeded")
        return instance
