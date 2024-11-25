from typing import Any

import pytest
from sqlalchemy import Engine, and_, asc, desc, or_
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import BinaryExpression

from photon.crud.errors import DoesNotExistError
from photon.crud.generic import session_factory
from tests.crud import (
    CourseCRUD,
    Dummy,
    GroupCRUD,
    LockerCRUD,
    StudentCRUD,
)
from tests.models import Course, Group, Student
from tests.types import SideEffect


@pytest.mark.parametrize(
    "attrs,expected",
    [
        ({}, {}),
        ({"id": 1}, {}),
        ({"id": 1, "foo": "spam"}, {"foo": "spam"}),
        (
            {"id": 1, "foo": "spam", "bar": "eggs", "baz": "ham"},
            {"foo": "spam", "bar": "eggs", "baz": "ham"},
        ),
    ],
)
def test_if_can_sanitize_unsafe_input_when_creates_instance(
    attrs: dict[str, Any], expected: dict[str, Any]
) -> None:
    dummy = Dummy.new(**attrs)

    for k in dummy._get_primary_key():
        assert k not in dummy.__dict__

    for k in expected:
        assert k in dummy.__dict__


@pytest.mark.parametrize(
    "attrs,expected",
    [
        ({}, {}),
        ({"id": 1}, {}),
        ({"id": 1, "foo": "spam"}, {"foo": "spam"}),
        (
            {"id": 1, "foo": "spam", "bar": "eggs", "baz": "ham"},
            {"foo": "spam", "bar": "eggs", "baz": "ham"},
        ),
    ],
)
def test_if_can_sanitize_unsafe_input_when_updates_instance(
    attrs: dict[str, Any], expected: dict[str, Any]
) -> None:
    dummy = Dummy(id=42)

    pk_before = {k: dummy.__dict__.get(k) for k in dummy._get_primary_key()}

    dummy.update(**attrs)

    pk_after = {k: dummy.__dict__.get(k) for k in dummy._get_primary_key()}

    assert pk_after == pk_before

    for k, v in expected.items():
        assert dummy.__dict__.get(k) == v


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

    created = group_crud.create(payload={"title": "ABC"})
    assert all([created.id, created.created_on, created.updated_on])
    assert created.title == "ABC"


def test_if_can_create_multiple_records(session: Session) -> None:
    group_crud = GroupCRUD(session=session)

    payload = [{"title": f"ABC_{index}"} for index in range(0, 5)]
    created = group_crud.create_many(payload=payload)
    for payload_item, instance in zip(payload, created):
        assert all([instance.id, instance.created_on, instance.updated_on])
        assert instance.title == payload_item["title"]


def test_if_raises_exception_when_retrieves_nonexistent_record(
    session: Session,
) -> None:
    with pytest.raises(DoesNotExistError) as error:
        GroupCRUD(session=session).read(id=42)

    assert (
        str(error.value)
        == "Instance of model='<class 'tests.models.Group'>' with id='42' was not found"
    )


def test_if_can_read_single_record(session: Session, content: SideEffect) -> None:
    student_crud = StudentCRUD(session=session)

    retrieved = student_crud.read(id=1)
    assert retrieved.id == 1


def test_if_can_read_multiple_records(session: Session, content: SideEffect) -> None:
    student_crud = StudentCRUD(session=session)

    retrieved = student_crud.read_many()
    assert {item.id for item in retrieved} == {1, 2, 3, 4, 5, 6, 7}


def test_if_can_sort_records(session: Session, content: SideEffect) -> None:
    student_crud = StudentCRUD(session=session)

    retrieved = student_crud.read_many(order_by=asc(Student.id))
    assert [item.id for item in retrieved] == [1, 2, 3, 4, 5, 6, 7]

    retrieved = student_crud.read_many(order_by=desc(Student.id))
    assert [item.id for item in retrieved] == [7, 6, 5, 4, 3, 2, 1]


def test_if_can_limit_records(session: Session, content: SideEffect) -> None:
    student_crud = StudentCRUD(session=session)

    retrieved = student_crud.read_many(take=1)
    assert {item.id for item in retrieved} == {1}

    retrieved = student_crud.read_many(take=5)
    assert {item.id for item in retrieved} == {1, 2, 3, 4, 5}


def test_if_can_offset_records(session: Session, content: SideEffect) -> None:
    student_crud = StudentCRUD(session=session)

    retrieved = student_crud.read_many(skip=2, take=2, order_by=asc(Student.id))
    assert [item.id for item in retrieved] == [3, 4]

    retrieved = student_crud.read_many(skip=2, take=2, order_by=desc(Student.id))
    assert [item.id for item in retrieved] == [5, 4]


def test_if_can_filter_records(session: Session, content: SideEffect) -> None:
    student_crud = StudentCRUD(session=session)

    retrieved = student_crud.read_many(where=Student.id > 4)
    assert {item.id for item in retrieved} == {5, 6, 7}

    retrieved = student_crud.read_many(where=or_(Student.id == 1, Student.id == 1024))
    assert {item.id for item in retrieved} == {1}

    retrieved = student_crud.read_many(
        where=and_(Student.id == 1, Student.name == "S1_G1")
    )
    instances = list(retrieved)
    assert len(instances) == 1
    instance = instances[0]
    assert instance.id == 1
    assert instance.name == "S1_G1"

    retrieved = student_crud.read_many(
        where=Student.id.in_([1, 7, 1024]), order_by=asc(Student.id)
    )
    assert [item.id for item in retrieved] == [1, 7]

    retrieved = student_crud.read_many(
        where=Student.group.has(Group.title.in_(["2", "1024"]))
    )
    assert all([instance.group.title == "2" for instance in retrieved])


def test_if_can_update_record(engine: Engine) -> None:
    with session_factory(bind=engine) as session:
        created = GroupCRUD(session=session).create(payload={"title": "ABC"})

    assert created.id
    assert created.updated_on
    assert created.updated_on
    assert created.title == "ABC"

    with session_factory(bind=engine) as session:
        updated = GroupCRUD(session=session).update(
            id=created.id, payload={"id": 42, "title": "DEF"}
        )

    assert updated.id
    assert updated.created_on
    assert updated.updated_on
    assert updated.id == created.id
    assert updated.title == "DEF"
    assert updated.created_on == created.created_on
    assert updated.updated_on >= created.updated_on


def test_if_can_get_one_to_one_related_field(
    engine: Engine, content: SideEffect
) -> None:
    with session_factory(bind=engine) as session:
        student = StudentCRUD(session=session).read(id=1)
        assert student.id == 1
        assert student.locker.id == 1

    with session_factory(bind=engine) as session:
        locker = LockerCRUD(session=session).read(id=1)
        assert locker.id == 1
        assert locker.student.id == 1


def test_if_can_get_one_to_many_related_field(
    engine: Engine, content: SideEffect
) -> None:
    with session_factory(bind=engine) as session:
        group = GroupCRUD(session=session).read(id=1)
        assert group.id == 1
        assert {student.id for student in group.students} == {1, 2, 3, 4, 5}


def test_if_can_get_many_to_many_related_field(
    engine: Engine, content: SideEffect
) -> None:
    with session_factory(bind=engine) as session:
        student = StudentCRUD(session=session).read(id=1)
        assert student.id == 1
        assert {course.id for course in student.courses} == {1, 2}

    with session_factory(bind=engine) as session:
        course = CourseCRUD(session=session).read(id=1)
        assert course.id == 1
        assert {student.id for student in course.students} == {1, 4, 5, 6, 7}
