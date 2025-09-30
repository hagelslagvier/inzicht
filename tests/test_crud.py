from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import Engine, and_, asc, desc, or_
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import BinaryExpression

from inzicht import GenericCRUD, session_factory
from inzicht.crud.errors import DoesNotExistError, IntegrityError
from tests.aliases import SideEffect
from tests.crud import (
    CourseCRUD,
    GroupCRUD,
    LockerCRUD,
    StudentCRUD,
)
from tests.models import Course, Group, Student


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
def test_if_can_count_records(
    session: Session, content: SideEffect, where: BinaryExpression | None, expected: int
) -> None:
    course_crud = CourseCRUD(session=session)

    count = course_crud.count(where=where)
    assert count == expected


def test_if_can_create_single_record(session: Session) -> None:
    group_crud = GroupCRUD(session=session)

    created = group_crud.create(title="ABC")
    assert all([created.id, created.created_on, created.updated_on])
    assert created.title == "ABC"


def test_if_can_create_multiple_records(session: Session) -> None:
    group_crud = GroupCRUD(session=session)

    required = [{"title": f"ABC_{index}"} for index in range(0, 64)]
    created = [group_crud.create(**item) for item in required]
    for required_item, created_item in zip(required, created):
        assert all([created_item.id, created_item.created_on, created_item.updated_on])
        assert created_item.title == required_item["title"]


def test_if_can_bulk_create_multiple_records(session: Session) -> None:
    group_crud = GroupCRUD(session=session)

    required = [Group(title=f"ABC_{index}") for index in range(0, 64)]
    created = group_crud.bulk_create(required)
    for required_item, created_item in zip(required, created):
        assert all([created_item.id, created_item.created_on, created_item.updated_on])
        assert created_item.title == required_item.title


def test_if_can_read_single_record(session: Session, content: SideEffect) -> None:
    student_crud = StudentCRUD(session=session)

    retrieved = student_crud.get(1)
    assert retrieved.id == 1


def test_if_can_read_multiple_records(session: Session, content: SideEffect) -> None:
    student_crud = StudentCRUD(session=session)

    retrieved = student_crud.read()
    assert {item.id for item in retrieved} == {1, 2, 3, 4, 5, 6, 7}


def test_if_can_sort_records(session: Session, content: SideEffect) -> None:
    student_crud = StudentCRUD(session=session)

    retrieved = student_crud.read(order_by=asc(Student.id))
    assert [item.id for item in retrieved] == [1, 2, 3, 4, 5, 6, 7]

    retrieved = student_crud.read(order_by=desc(Student.id))
    assert [item.id for item in retrieved] == [7, 6, 5, 4, 3, 2, 1]


def test_if_can_limit_records(session: Session, content: SideEffect) -> None:
    student_crud = StudentCRUD(session=session)

    retrieved = student_crud.read(take=1)
    assert {item.id for item in retrieved} == {1}

    retrieved = student_crud.read(take=5)
    assert {item.id for item in retrieved} == {1, 2, 3, 4, 5}


def test_if_can_offset_records(session: Session, content: SideEffect) -> None:
    student_crud = StudentCRUD(session=session)

    retrieved = student_crud.read(skip=2, take=2, order_by=asc(Student.id))
    assert [item.id for item in retrieved] == [3, 4]

    retrieved = student_crud.read(skip=2, take=2, order_by=desc(Student.id))
    assert [item.id for item in retrieved] == [5, 4]


def test_if_can_filter_records(session: Session, content: SideEffect) -> None:
    student_crud = StudentCRUD(session=session)

    retrieved = student_crud.read(where=Student.id > 4)
    assert {item.id for item in retrieved} == {5, 6, 7}

    retrieved = student_crud.read(where=or_(Student.id == 1, Student.id == 1024))
    assert {item.id for item in retrieved} == {1}

    retrieved = student_crud.read(where=and_(Student.id == 1, Student.name == "S1_G1"))
    instances = list(retrieved)
    assert len(instances) == 1
    instance = instances[0]
    assert instance.id == 1
    assert instance.name == "S1_G1"

    retrieved = student_crud.read(
        where=Student.id.in_([1, 7, 1024]), order_by=asc(Student.id)
    )
    assert [item.id for item in retrieved] == [1, 7]

    retrieved = student_crud.read(
        where=Student.group.has(Group.title.in_(["2", "1024"]))
    )
    assert all([instance.group.title == "2" for instance in retrieved])


def test_if_can_update_record(engine: Engine) -> None:
    with session_factory(bind=engine) as session:
        created = GroupCRUD(session=session).create(title="ABC")

    assert created.id
    assert created.updated_on
    assert created.updated_on
    assert created.title == "ABC"

    with session_factory(bind=engine) as session:
        updated = GroupCRUD(session=session).update(created.id, title="DEF")

    assert updated.id
    assert updated.created_on
    assert updated.updated_on
    assert updated.id == created.id
    assert updated.title == "DEF"
    assert updated.created_on == created.created_on
    assert updated.updated_on >= created.updated_on


def test_if_can_update_via_attributes(engine: Engine, content: SideEffect) -> None:
    with session_factory(bind=engine) as session:
        student = StudentCRUD(session=session).get(1)

        assert student.id == 1
        assert student.name == "S1_G1"
        assert {course.id for course in student.courses} == {1, 2}

    with session_factory(bind=engine) as session:
        course_crud = CourseCRUD(session=session)
        course_1 = course_crud.get(1)
        course_5 = course_crud.get(5)

        student = StudentCRUD(session=session).get(1)
        student.courses.remove(course_1)
        student.courses.append(course_5)

        student.name = "Updated"

    with session_factory(bind=engine) as session:
        student = StudentCRUD(session=session).get(1)

        assert student.name == "Updated"
        assert {course.id for course in student.courses} == {2, 5}
        assert student.created_on
        assert student.updated_on
        assert student.updated_on > student.created_on


def test_if_can_get_one_to_one_related_field(
    engine: Engine, content: SideEffect
) -> None:
    with session_factory(bind=engine) as session:
        student = StudentCRUD(session=session).get(1)
        assert student.id == 1
        assert student.locker.id == 1

    with session_factory(bind=engine) as session:
        locker = LockerCRUD(session=session).get(1)
        assert locker.id == 1
        assert locker.student.id == 1


def test_if_can_get_one_to_many_related_field(
    engine: Engine, content: SideEffect
) -> None:
    with session_factory(bind=engine) as session:
        group = GroupCRUD(session=session).get(1)
        assert group.id == 1
        assert {student.id for student in group.students} == {1, 2, 3, 4, 5}


def test_if_can_get_many_to_many_related_field(
    engine: Engine, content: SideEffect
) -> None:
    with session_factory(bind=engine) as session:
        student = StudentCRUD(session=session).get(1)
        assert student.id == 1
        assert {course.id for course in student.courses} == {1, 2}

    with session_factory(bind=engine) as session:
        course = CourseCRUD(session=session).get(1)
        assert course.id == 1
        assert {student.id for student in course.students} == {1, 4, 5, 6, 7}


def test_if_can_delete_record(engine: Engine, content: SideEffect) -> None:
    with session_factory(bind=engine) as session:
        course_crud = CourseCRUD(session=session)

        count = course_crud.count()
        assert count == 5

        retrieved = course_crud.get(1)
        assert retrieved.id == 1

        deleted = course_crud.delete(1)
        assert deleted.id == 1

        count = course_crud.count()
        assert count == 4


def test_if_can_rollback_transaction_when_error_occurs(engine: Engine) -> None:
    with patch("inzicht.crud.factories.Session") as session_factory_mock:
        session_mock = MagicMock()
        session_mock.begin.side_effect = Exception("Boooom!")
        session_context_mock = MagicMock()
        session_context_mock.__enter__.return_value = session_mock
        session_factory_mock.return_value = session_context_mock

        with pytest.raises(Exception) as error:
            with session_factory(bind=engine) as session:
                StudentCRUD(session=session).get(1)

        assert str(error.value) == "Boooom!"

        session_mock.begin.assert_called_once()
        session_mock.rollback.assert_called_once()


def test_if_can_parameterize_at_instantiation(
    engine: Engine, content: SideEffect
) -> None:
    with session_factory(bind=engine) as session:
        group_crud = GenericCRUD[Group](session=session)
        group = group_crud.create(title="foo_bar_baz")
        created_id = group.id

        assert created_id

    with session_factory(bind=engine) as session:
        group_crud = GenericCRUD[Group](session=session)
        group = group_crud.get(created_id)

        assert group.id == created_id

    with session_factory(bind=engine) as session:
        group_crud = GenericCRUD[Group](session=session)
        retrieved = group_crud.read(where=Group.title == "foo_bar_baz")
        (found,) = list(retrieved)

        assert found.id == created_id

    with session_factory(bind=engine) as session:
        group_crud = GenericCRUD[Group](session=session)
        updated = group_crud.update(created_id, title="baz_bar_foo")
        assert updated.id == created_id
        assert updated.title == "baz_bar_foo"

    with session_factory(bind=engine) as session:
        group_crud = GenericCRUD[Group](session=session)
        group = group_crud.get(created_id)

        assert group.id == updated.id
        assert group.title == "baz_bar_foo"

    with session_factory(bind=engine) as session:
        group_crud = GenericCRUD[Group](session=session)
        deleted = group_crud.delete(created_id)

        assert deleted.id == created_id

    with session_factory(bind=engine) as session:
        group_crud = GenericCRUD[Group](session=session)
        with pytest.raises(DoesNotExistError) as error:
            group_crud.get(created_id)

        assert (
            str(error.value)
            == f"DB operation [GET] on instance of model '<class 'tests.models.Group'>' with id '3' failed because the instance was not found"
        )
        assert error.value.kwargs == dict(id=created_id)


def test_if_raises_integrity_error_when_creating_single_instance_given_unique_constraint_violated(
    engine: Engine,
) -> None:
    with session_factory(bind=engine) as session:
        group_crud = GenericCRUD[Group](session=session)
        group_crud.create(title="foo_bar_baz")

    with pytest.raises(IntegrityError) as error:
        with session_factory(bind=engine) as session:
            group_crud = GenericCRUD[Group](session=session)
            group_crud.create(title="foo_bar_baz")

    assert error.value.kwargs == dict(title="foo_bar_baz")


def test_if_raises_integrity_error_when_creating_multiple_instances_given_unique_constraint_violated(
    engine: Engine,
) -> None:
    with pytest.raises(IntegrityError):
        with session_factory(bind=engine) as session:
            group_crud = GenericCRUD[Group](session=session)
            group_crud.bulk_create(
                [Group(title=f"foo_bar_baz"), Group(title=f"foo_bar_baz")]
            )


def test_if_raises_error_when_reading_nonexistent_instance(
    session: Session, content: SideEffect
) -> None:
    with pytest.raises(DoesNotExistError) as error:
        group_crud = GenericCRUD[Group](session=session)
        group_crud.get(42)

    assert (
        str(error.value)
        == "DB operation [GET] on instance of model '<class 'tests.models.Group'>' with id '42' failed because the instance was not found"
    )
    assert error.value.kwargs == dict(id=42)


def test_if_raises_error_when_updating_nonexistent_instance(
    session: Session, content: SideEffect
) -> None:
    with pytest.raises(DoesNotExistError) as error:
        group_crud = GenericCRUD[Group](session=session)
        group_crud.update(42, title="foo")

    assert (
        str(error.value)
        == "DB operation [UPDATE] on instance of model '<class 'tests.models.Group'>' with id '42' failed because the instance was not found"
    )
    assert error.value.kwargs == dict(id=42, title="foo")


def test_if_raises_integrity_error_when_updating_instance_given_unique_constraint_violated(
    engine: Engine,
) -> None:
    with session_factory(bind=engine) as session:
        group_crud = GenericCRUD[Group](session=session)
        g1 = group_crud.create(title="foo_bar_baz_1")
        g2 = group_crud.create(title="foo_bar_baz_2")

    with pytest.raises(IntegrityError):
        with session_factory(bind=engine) as session:
            group_crud = GenericCRUD[Group](session=session)
            group_crud.update(g2.id, title="foo_bar_baz_1")


def test_if_raises_error_when_deleting_nonexistent_instance(
    session: Session, content: SideEffect
) -> None:
    with pytest.raises(DoesNotExistError) as error:
        group_crud = GenericCRUD[Group](session=session)
        group_crud.delete(42)

    assert (
        str(error.value)
        == "DB operation [GET] on instance of model '<class 'tests.models.Group'>' with id '42' failed because the instance was not found"
    )
    assert error.value.kwargs == dict(id=42)
