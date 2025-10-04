import datetime

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Table,
    asc,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from inzicht.declarative import DeclarativeBase

_now = lambda: datetime.datetime.now()


class Base(DeclarativeBase):
    __abstract__ = True

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_on: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_now
    )
    updated_on: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now
    )


m2m_student_course = Table(
    "m2m_student_course",
    Base.metadata,
    Column(
        "student_id",
        Integer,
        ForeignKey("students.id"),
        primary_key=True,
        nullable=True,
    ),
    Column(
        "course_id", Integer, ForeignKey("courses.id"), primary_key=True, nullable=True
    ),
)


class Group(Base):
    __tablename__ = "groups"

    title: Mapped[str] = mapped_column(String(8), unique=True)
    students: Mapped[list["Student"]] = (
        relationship(  # Mapped[List[<model>]] -> one-to-many
            back_populates="group", order_by=asc(text("students.id")), lazy="selectin"
        )
    )

    def __repr__(self) -> str:
        return f"Group(id={self.id})"


class Student(Base):
    __tablename__ = "students"

    name: Mapped[str] = mapped_column(String(64), unique=True)
    group_id: Mapped[int] = mapped_column(ForeignKey("groups.id"))
    group: Mapped["Group"] = relationship(back_populates="students", lazy="selectin")
    locker_id: Mapped[int] = mapped_column(ForeignKey("lockers.id"))
    locker: Mapped["Locker"] = relationship(back_populates="student", lazy="selectin")

    courses: Mapped[list["Course"]] = relationship(
        secondary=m2m_student_course,  # many-to-many
        back_populates="students",
        order_by=asc(text("courses.id")),
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"Student(id={self.id})"


class Course(Base):
    __tablename__ = "courses"

    title: Mapped[str] = mapped_column(String(64), unique=True)

    students: Mapped[list["Student"]] = relationship(
        secondary=m2m_student_course,  # many-to-many
        back_populates="courses",
        order_by=asc(text("students.id")),
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"Course(id={self.id})"


class Locker(Base):
    __tablename__ = "lockers"

    code: Mapped[str] = mapped_column(String(16))
    student: Mapped[Student] = relationship(
        back_populates="locker", lazy="selectin"
    )  # Mapped[<model>] -> one-to-one

    def __repr__(self) -> str:
        return f"Locker(id={self.id})"


class Dummy(Base):
    __tablename__ = "dummies"

    foo = mapped_column(String(8), unique=True, nullable=True)
    bar = mapped_column(String(8), unique=True, nullable=True)
    baz = mapped_column(String(8), unique=True, nullable=True)
