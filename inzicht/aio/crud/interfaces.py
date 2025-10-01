from abc import ABC, abstractmethod
from collections.abc import Generator, Sequence
from typing import Any, Generic, TypeVar, overload

from inzicht.declarative import DeclarativeBase

T = TypeVar("T", bound=DeclarativeBase)


class AioCRUDInterface(ABC, Generic[T]):
    """
    Abstract base class that defines the CRUD interface for generic operations on a resource of type T.
    """

    @abstractmethod
    def get_model(self) -> type[T]:
        """
        Retrieves the model class associated with the GenericCRUD class.

        Returns:
            type[T]: The model class associated with the GenericCRUD class.
        """

    @abstractmethod
    async def count(self, where: Any | None = None) -> int:
        """
        Count the total number of records.

        Args:
            where (Any, optional): Filter conditions for retrieving records.

        Returns:
            int: The total number of records in the collection.
        """

    @overload
    @abstractmethod
    async def create(self, instance: T, /) -> T:
        pass

    @overload
    @abstractmethod
    async def create(self, /, **kwargs: Any) -> T:
        pass

    @abstractmethod
    async def create(self, instance: T | None = None, /, **kwargs: Any) -> T:
        """
        Create a new record from the provided keyword-arguments.

        Args:
        instance (T): An instance of the model to be created.
        **kwargs (Any): The attributes to initialize the model instance with.

        Returns:
            T: The created record.
        """

    @abstractmethod
    async def bulk_create(self, instances: Sequence[T], /) -> Sequence[T]:
        """
        Create multiple records from the provided instances.

        Args:
            instances (Sequence[T]): A sequence of items to be added to the database.

        Returns:
            Sequence[T]: A sequence of created records.
        """

    @abstractmethod
    async def get(self, id: int | str, /) -> T:
        """
        Retrieve a single record by its ID.

        Args:
            id (int | str): The ID of the record to retrieve.

        Returns:
            T: The record with the specified ID.
        """

    @abstractmethod
    async def read(
        self,
        *,
        where: Any | None = None,
        order_by: Any | None = None,
        skip: int = 0,
        take: int | None = None,
    ) -> Generator[T, None, None]:
        """
        Retrieve multiple records based on conditions.

        Args:
            where (Any, optional): Filter conditions for retrieving records.
            order_by (Any, optional): Criteria to order the results.
            skip (int, optional): Number of records to skip. Defaults to 0.
            take (int, optional): Number of records to retrieve. Defaults to None, which mean no limit.

        Returns:
            Generator[T, None, None]: A generator of the retrieved records.
        """

    @abstractmethod
    async def update(self, id: int | str, /, **kwargs: Any) -> T:
        """
        Update a record by its ID with the provided payload.

        Args:
            id (int | str): The ID of the record to update.
            kwargs (Any): The attributes to update the record with.

        Returns:
            T: The updated record.
        """

    @abstractmethod
    async def delete(self, id: int | str, /) -> T:
        """
        Delete a record by its ID.

        Args:
            id (int | str): The ID of the record to delete.

        Returns:
            T: The deleted record.
        """
