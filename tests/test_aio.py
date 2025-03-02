import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import and_, asc, desc, or_
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.sql.elements import BinaryExpression

from inzicht.aio.crud.factories import async_session_factory
from inzicht.crud.errors import DoesNotExistError
from tests.aio.crud import AioCourseCRUD, AioGroupCRUD, AioLockerCRUD, AioStudentCRUD
from tests.aliases import SideEffect
from tests.models import Course, Group, Student


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "where, expected",
    [
        (None, 5),
        (Course.id == 1, 1),
        (Course.title == "Course_1", 1),
        (or_(Course.title == "Course_1", Course.title == "Course_2"), 2),
        (and_(Course.title == "Course_1", Course.title == "Course_2"), 0),
    ],
)
async def test_if_can_async_count_records(
    async_session: AsyncSession,
    async_content: SideEffect,
    where: BinaryExpression | None,
    expected: int,
) -> None:
    course_crud = AioCourseCRUD(async_session=async_session)

    count = await course_crud.count(where=where)
    assert count == expected


@pytest.mark.asyncio
async def test_if_can_async_create_single_record(async_session: AsyncSession) -> None:
    group_crud = AioGroupCRUD(async_session=async_session)

    created = await group_crud.create(title="ABC")
    assert all([created.id, created.created_on, created.updated_on])
    assert created.title == "ABC"


@pytest.mark.asyncio
async def test_if_can_async_create_multiple_records_sequentially(
    async_session: AsyncSession,
) -> None:
    group_crud = AioGroupCRUD(async_session=async_session)

    requested = [{"title": f"ABC_{index}"} for index in range(0, 64)]
    created = []
    for item in requested:
        group = await group_crud.create(**item)
        created.append(group)

    for requested_item, created_item in zip(requested, created):
        assert all([created_item.id, created_item.created_on, created_item.updated_on])
        assert created_item.title == requested_item["title"]


@pytest.mark.asyncio
async def test_if_can_async_create_multiple_records_concurrently(
    async_session: AsyncSession,
) -> None:
    group_crud = AioGroupCRUD(async_session=async_session)

    requested = [{"title": f"ABC_{index}"} for index in range(0, 64)]

    created = await asyncio.gather(*[group_crud.create(**item) for item in requested])

    for requested_item, created_item in zip(requested, created):
        assert all([created_item.id, created_item.created_on, created_item.updated_on])
        assert created_item.title == requested_item["title"]


@pytest.mark.asyncio
async def test_if_can_async_bulk_create_multiple_records(
    async_session: AsyncSession,
) -> None:
    group_crud = AioGroupCRUD(async_session=async_session)

    required = [Group(title=f"ABC_{index}") for index in range(0, 64)]
    created = await group_crud.bulk_create(required)
    for required_item, created_item in zip(required, created):
        assert all([created_item.id, created_item.created_on, created_item.updated_on])
        assert created_item.title == required_item.title


@pytest.mark.asyncio
async def test_if_raises_async_exception_when_retrieves_nonexistent_record(
    async_session: AsyncSession,
) -> None:
    with pytest.raises(DoesNotExistError) as error:
        await AioGroupCRUD(async_session=async_session).get(42)

    assert (
        str(error.value)
        == "Instance of model='<class 'tests.models.Group'>' with id='42' was not found"
    )


@pytest.mark.asyncio
async def test_if_can_async_read_single_record(
    async_session: AsyncSession, async_content: SideEffect
) -> None:
    student_crud = AioStudentCRUD(async_session=async_session)

    retrieved = await student_crud.get(1)
    assert retrieved.id == 1


@pytest.mark.asyncio
async def test_if_can_async_read_multiple_records(
    async_session: AsyncSession, async_content: SideEffect
) -> None:
    student_crud = AioStudentCRUD(async_session=async_session)

    retrieved = await student_crud.read()
    assert {item.id for item in retrieved} == {1, 2, 3, 4, 5, 6, 7}


@pytest.mark.asyncio
async def test_if_can_async_sort_records(
    async_session: AsyncSession, async_content: SideEffect
) -> None:
    student_crud = AioStudentCRUD(async_session=async_session)

    retrieved = await student_crud.read(order_by=asc(Student.id))
    assert [item.id for item in retrieved] == [1, 2, 3, 4, 5, 6, 7]

    retrieved = await student_crud.read(order_by=desc(Student.id))
    assert [item.id for item in retrieved] == [7, 6, 5, 4, 3, 2, 1]


@pytest.mark.asyncio
async def test_if_can_async_limit_records(
    async_session: AsyncSession, async_content: SideEffect
) -> None:
    student_crud = AioStudentCRUD(async_session=async_session)

    retrieved = await student_crud.read(take=1)
    assert {item.id for item in retrieved} == {1}

    retrieved = await student_crud.read(take=5)
    assert {item.id for item in retrieved} == {1, 2, 3, 4, 5}


@pytest.mark.asyncio
async def test_if_can_async_offset_records(
    async_session: AsyncSession, async_content: SideEffect
) -> None:
    student_crud = AioStudentCRUD(async_session=async_session)

    retrieved = await student_crud.read(skip=2, take=2, order_by=asc(Student.id))
    assert [item.id for item in retrieved] == [3, 4]

    retrieved = await student_crud.read(skip=2, take=2, order_by=desc(Student.id))
    assert [item.id for item in retrieved] == [5, 4]


@pytest.mark.asyncio
async def test_if_can_async_filter_records(
    async_session: AsyncSession, async_content: SideEffect
) -> None:
    student_crud = AioStudentCRUD(async_session=async_session)

    retrieved = await student_crud.read(where=Student.id > 4)
    assert {item.id for item in retrieved} == {5, 6, 7}

    retrieved = await student_crud.read(where=or_(Student.id == 1, Student.id == 1024))
    assert {item.id for item in retrieved} == {1}

    retrieved = await student_crud.read(
        where=and_(Student.id == 1, Student.name == "S1_G1")
    )
    instances = list(retrieved)
    assert len(instances) == 1
    instance = instances[0]
    assert instance.id == 1
    assert instance.name == "S1_G1"

    retrieved = await student_crud.read(
        where=Student.id.in_([1, 7, 1024]), order_by=asc(Student.id)
    )
    assert [item.id for item in retrieved] == [1, 7]

    retrieved = await student_crud.read(
        where=Student.group.has(Group.title.in_(["2", "1024"]))
    )

    assert all([instance.group.title == "2" for instance in retrieved])


@pytest.mark.asyncio
async def test_if_can_async_update_record(async_engine: AsyncEngine) -> None:
    async with async_session_factory(bind=async_engine) as async_session:
        created = await AioGroupCRUD(async_session=async_session).create(title="ABC")

    assert created.id
    assert created.updated_on
    assert created.updated_on
    assert created.title == "ABC"

    async with async_session_factory(bind=async_engine) as async_session:
        updated = await AioGroupCRUD(async_session=async_session).update(
            created.id, title="DEF"
        )

    assert updated.id
    assert updated.created_on
    assert updated.updated_on
    assert updated.id == created.id
    assert updated.title == "DEF"
    assert updated.created_on == created.created_on
    assert updated.updated_on >= created.updated_on


@pytest.mark.asyncio
async def test_if_can_async_update_via_attributes(
    async_engine: AsyncEngine, async_content: SideEffect
) -> None:
    async with async_session_factory(bind=async_engine) as async_session:
        student = await AioStudentCRUD(async_session=async_session).get(1)

        assert student.id == 1
        assert student.name == "S1_G1"
        assert {course.id for course in student.courses} == {1, 2}

    async with async_session_factory(bind=async_engine) as async_session:
        course_crud = AioCourseCRUD(async_session=async_session)
        course_1 = await course_crud.get(1)
        course_5 = await course_crud.get(5)

        student_crud = AioStudentCRUD(async_session=async_session)
        student = await student_crud.get(1)
        courses = await student.awaitable_attrs.courses
        courses.remove(course_1)
        courses.append(course_5)

        student.name = "Updated"

    async with async_session_factory(bind=async_engine) as async_session:
        student_crud = AioStudentCRUD(async_session=async_session)
        student = await student_crud.get(1)

        assert student.name == "Updated"
        assert {course.id for course in student.courses} == {2, 5}
        assert student.created_on
        assert student.updated_on
        assert student.updated_on > student.created_on


@pytest.mark.asyncio
async def test_if_can_async_get_one_to_one_related_field(
    async_engine: AsyncEngine, async_content: SideEffect
) -> None:
    async with async_session_factory(bind=async_engine) as async_session:
        student = await AioStudentCRUD(async_session=async_session).get(1)
        assert student.id == 1
        assert student.locker.id == 1

    async with async_session_factory(bind=async_engine) as async_session:
        locker = await AioLockerCRUD(async_session=async_session).get(1)
        assert locker.id == 1
        assert locker.student.id == 1


@pytest.mark.asyncio
async def test_if_can_async_get_one_to_many_related_field(
    async_engine: AsyncEngine, async_content: SideEffect
) -> None:
    async with async_session_factory(bind=async_engine) as async_session:
        group = await AioGroupCRUD(async_session=async_session).get(1)
        assert group.id == 1
        assert {student.id for student in group.students} == {1, 2, 3, 4, 5}


@pytest.mark.asyncio
async def test_if_can_async_get_many_to_many_related_field(
    async_engine: AsyncEngine, async_content: SideEffect
) -> None:
    async with async_session_factory(bind=async_engine) as async_session:
        student = await AioStudentCRUD(async_session=async_session).get(1)
        assert student.id == 1
        assert {course.id for course in student.courses} == {1, 2}

    async with async_session_factory(bind=async_engine) as async_session:
        course = await AioCourseCRUD(async_session=async_session).get(1)
        assert course.id == 1
        assert {student.id for student in course.students} == {1, 4, 5, 6, 7}


@pytest.mark.asyncio
async def test_if_can_async_delete_record(
    async_engine: AsyncEngine, async_content: SideEffect
) -> None:
    async with async_session_factory(bind=async_engine) as async_session:
        course_crud = AioCourseCRUD(async_session=async_session)

        count = await course_crud.count()
        assert count == 5

        retrieved = await course_crud.get(1)
        assert retrieved.id == 1

        deleted = await course_crud.delete(1)
        assert deleted.id == 1

        count = await course_crud.count()
        assert count == 4


@pytest.mark.asyncio
async def test_if_can_async_rollback_transaction_when_error_occurs(
    async_engine: AsyncEngine,
) -> None:
    with patch("inzicht.aio.crud.factories.AsyncSession") as async_session_factory_mock:
        session_mock = AsyncMock()
        session_mock.begin.side_effect = Exception("Boooom!")
        session_context_mock = MagicMock()
        session_context_mock.__aenter__.return_value = session_mock
        async_session_factory_mock.return_value = session_context_mock

        with pytest.raises(Exception) as error:
            async with async_session_factory(bind=async_engine) as async_session:
                await AioStudentCRUD(async_session=async_session).get(1)

        assert str(error.value) == "Boooom!"

        session_mock.begin.assert_called_once()
        session_mock.rollback.assert_called_once()
