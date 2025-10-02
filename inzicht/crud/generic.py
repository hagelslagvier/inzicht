import logging
from collections.abc import Generator, Sequence
from typing import Any, TypeVar, get_args

import sqlalchemy
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from inzicht.crud.errors import DoesNotExistError, IntegrityError, UnknowError
from inzicht.crud.interfaces import CRUDInterface
from inzicht.declarative import DeclarativeBase

T = TypeVar("T", bound=DeclarativeBase)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)

logger = logging.getLogger("crud.generic")
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class GenericCRUD(CRUDInterface[T]):
    def __init__(self, session: Session) -> None:
        self.session = session

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

    def count(self, where: Any | None = None) -> int:
        model = self.get_model()
        query = select(func.count()).select_from(model)
        if where is not None:
            query = query.filter(where)
        count = self.session.execute(query).scalar() or 0
        return count

    def create(self, instance: T | None = None, /, **kwargs: Any) -> T:
        model = self.get_model()
        if instance and kwargs:
            raise ValueError(
                "Cannot provide both 'instance' and keyword arguments for creation"
            )
        instance = instance or model.new(**kwargs)
        header = f"DB operation [CREATE] on instance '{instance}' of model '{model}'"
        try:
            self.session.add(instance)
            self.session.flush()
        except sqlalchemy.exc.IntegrityError as error:
            error_message = f"{header} failed because of integrity constraints"
            logger.error(error_message)
            raise IntegrityError(error_message, **kwargs) from error
        except Exception as error:
            error_message = f"{header} failed because of unknown error"
            logger.error(error_message)
            raise UnknowError(error_message, **kwargs) from error
        return instance

    def bulk_create(self, instances: Sequence[T]) -> Sequence[T]:
        model = self.get_model()
        header = f"DB operation [BULK_CREATE] on {len(instances)} instances '[{instances[0]},...]' of model '{model}'"
        try:
            self.session.add_all(instances)
            self.session.flush()
        except sqlalchemy.exc.IntegrityError as error:
            error_message = f"{header} failed because of integrity constraints"
            logger.error(error_message)
            raise IntegrityError(error_message) from error
        except Exception as error:
            error_message = f"{header} failed because of unknow error"
            logger.error(error_message)
            raise UnknowError(error_message) from error
        return instances

    def get(self, id: int | str, /) -> T:
        model = self.get_model()
        instance = self.session.get(model, id)
        header = f"DB operation [GET] on instance of model '{model}' with id '{id}'"
        if not instance:
            error_message = f"{header} failed because the instance was not found"
            logger.error(error_message)
            raise DoesNotExistError(error_message, id=id)
        return instance

    def read(
        self,
        *,
        where: Any | None = None,
        order_by: Any | None = None,
        skip: int = 0,
        take: int | None = None,
    ) -> Generator[T, None, None]:
        model = self.get_model()
        query = select(model)
        header = f"DB operation [GET] on instance of model '{model}' with id '{id}'"
        if where is not None:
            query = query.filter(where)
        if order_by is not None:
            query = query.order_by(order_by)
        if skip:
            query = query.offset(skip)
        if take:
            query = query.limit(take)
        items = (item for item in self.session.execute(query).scalars())
        logger.info(f"{header} succeeded")
        return items

    def update(self, id: int | str, /, **kwargs: Any) -> T:
        model = self.get_model()
        instance = self.session.get(model, id, with_for_update={"nowait": True})
        header = f"DB operation [UPDATE] on instance of model '{model}' with id '{id}'"
        if not instance:
            error_message = f"{header} failed because the instance was not found"
            logger.error(error_message)
            raise DoesNotExistError(error_message, id=id, **kwargs)
        instance.update(**kwargs)
        try:
            self.session.add(instance)
            self.session.flush()
        except sqlalchemy.exc.IntegrityError as error:
            error_message = f"{header} failed because of integrity constraints"
            logger.error(error_message)
            raise IntegrityError(error_message, **kwargs) from error
        except Exception as error:
            error_message = f"{header} failed because of unknow error"
            logger.error(error_message)
            raise UnknowError(error_message, **kwargs) from error
        return instance

    def delete(self, id: int | str, /) -> T:
        model = self.get_model()
        instance = self.get(id)
        header = f"DB operation [DELETE] on instance {instance} of model '{model}' with id '{id}'"
        if not instance:
            error_message = f"{header} failed because the instance was not found"
            logger.error(error_message)
            raise DoesNotExistError(error_message)
        self.session.delete(instance)
        self.session.flush()
        return instance
