from typing import Any

import pytest
from sqlalchemy.orm import Session

from inzicht import GenericCRUD
from tests.crud import (
    Course,
    Dummy,
    Group,
    Locker,
    Student,
)


@pytest.mark.parametrize("type_parameter", [Group, Student, Course, Locker, Dummy])
def test_if_can_get_model(type_parameter: Any, session: Session) -> None:
    crud = GenericCRUD[type_parameter](session=session)
    assert crud.get_model() is type_parameter


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
