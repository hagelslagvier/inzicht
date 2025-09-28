class BaseORMError(Exception):
    pass


class DoesNotExistError(BaseORMError):
    pass


class IntegrityError(BaseORMError):
    pass


class UnknowError(BaseORMError):
    pass
