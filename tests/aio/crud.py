from inzicht.aio.crud.generic import AioGenericCRUD
from tests.models import Course, Group, Locker, Student


class AioCourseCRUD(AioGenericCRUD[Course]):
    pass


class AioLockerCRUD(AioGenericCRUD[Locker]):
    pass


class AioGroupCRUD(AioGenericCRUD[Group]):
    pass


class AioStudentCRUD(AioGenericCRUD[Student]):
    pass
