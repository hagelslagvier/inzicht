from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from sqlalchemy import Engine, StaticPool, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import Session

from inzicht import DeclarativeBase, session_factory
from inzicht.aio.crud.factories import async_session_factory
from tests.aio.crud import AioCourseCRUD, AioGroupCRUD, AioLockerCRUD, AioStudentCRUD
from tests.crud import CourseCRUD, GroupCRUD, LockerCRUD, StudentCRUD


@pytest.fixture(scope="function")
def engine() -> Generator[Engine, None, None]:
    engine = create_engine(
        url="sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    metadata = DeclarativeBase.metadata
    metadata.drop_all(bind=engine)
    metadata.create_all(bind=engine)
    yield engine


@pytest_asyncio.fixture(scope="function")
async def async_engine() -> AsyncGenerator[AsyncEngine, None]:
    aengine = create_async_engine(url="sqlite+aiosqlite:///:memory:")
    metadata = DeclarativeBase.metadata
    async with aengine.begin() as connection:
        await connection.run_sync(metadata.drop_all)
        await connection.run_sync(metadata.create_all)

    yield aengine


@pytest.fixture(scope="function")
def session(engine: Engine) -> Generator[Session, None, None]:
    with session_factory(bind=engine) as session:
        yield session


@pytest_asyncio.fixture(scope="function")
async def async_session(
    async_engine: AsyncEngine,
) -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory(bind=async_engine) as asession:
        yield asession


@pytest.fixture(scope="function")
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


@pytest_asyncio.fixture
async def async_content(async_engine: AsyncEngine) -> None:
    async with async_session_factory(bind=async_engine) as asession:
        locker_crud = AioLockerCRUD(async_session=asession)
        course_crud = AioCourseCRUD(async_session=asession)
        group_crud = AioGroupCRUD(async_session=asession)
        student_crud = AioStudentCRUD(async_session=asession)

        lockers = [await locker_crud.create(code=index) for index in range(1, 8)]
        courses = [
            await course_crud.create(title=f"Course_{index}") for index in range(1, 6)
        ]
        groups = [await group_crud.create(title=index) for index in range(1, 3)]

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
            await student_crud.create(**student)
