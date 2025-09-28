from typing import Any


class BaseORMError(Exception):
    def __init__(self, message: str = "", **kwargs: Any):
        super().__init__(message)
        self.kwargs = kwargs


class DoesNotExistError(BaseORMError):
    pass


class IntegrityError(BaseORMError):
    pass


class UnknowError(BaseORMError):
    pass
