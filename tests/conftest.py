from collections.abc import Generator

import pytest
from sqlalchemy import Engine, StaticPool, create_engine
from sqlalchemy.orm import Session

from inzicht import DeclarativeBase, session_factory
from tests.crud import CourseCRUD, GroupCRUD, LockerCRUD, StudentCRUD


@pytest.fixture
def engine() -> Generator[Engine, None, None]:
    engine = create_engine(
        url="sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    metadata = DeclarativeBase.metadata
    metadata.drop_all(bind=engine)
    metadata.create_all(bind=engine)
    yield engine


@pytest.fixture
def session(engine: Engine) -> Generator[Session, None, None]:
    with session_factory(bind=engine) as session:
        yield session


@pytest.fixture
def content(engine: Engine) -> None:
    with session_factory(bind=engine) as session:
        locker_crud = LockerCRUD(session=session)
        course_crud = CourseCRUD(session=session)
        group_crud = GroupCRUD(session=session)
        student_crud = StudentCRUD(session=session)

        lockers = [locker_crud.create(code=index) for index in range(1, 8)]
        courses = [course_crud.create(title=f"Course_{index}") for index in range(1, 6)]
        groups = [group_crud.create(title=index) for index in range(1, 3)]

        students = [
            {
                "name": "S1_G1",
                "group": groups[0],
                "locker": lockers[0],
                "courses": [courses[0], courses[1]],
            },
            {
                "name": "S2_G1",
                "group": groups[0],
                "locker": lockers[1],
                "courses": [courses[1]],
            },
            {
                "name": "S3_G1",
                "group": groups[0],
                "locker": lockers[2],
                "courses": [courses[1]],
            },
            {
                "name": "S4_G1",
                "group": groups[0],
                "locker": lockers[3],
                "courses": [courses[0]],
            },
            {
                "name": "S5_G1",
                "group": groups[0],
                "locker": lockers[4],
                "courses": [courses[0], courses[2]],
            },
            {
                "name": "S1_G2",
                "group": groups[1],
                "locker": lockers[5],
                "courses": [courses[0], courses[3]],
            },
            {
                "name": "S2_G2",
                "group": groups[1],
                "locker": lockers[6],
                "courses": [courses[0], courses[4]],
            },
        ]
        for student in students:
            student_crud.create(**student)
