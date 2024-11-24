from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Table,
    asc,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from photon.declarative import DeclarativeBase


class Base(DeclarativeBase):
    __abstract__ = True

    id = mapped_column(Integer, primary_key=True)
    created_on = mapped_column(DateTime, default=func.now())
    updated_on = mapped_column(DateTime, default=func.now(), onupdate=func.now())


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
            back_populates="group", order_by=asc(text("students.id"))
        )
    )

    def __repr__(self) -> str:
        return f"Group(id={self.id})"


class Student(Base):
    __tablename__ = "students"

    name: Mapped[str] = mapped_column(String(64), unique=True)
    group_id: Mapped[int] = mapped_column(ForeignKey("groups.id"))
    group: Mapped["Group"] = relationship(
        back_populates="students",
    )
    locker_id: Mapped[int] = mapped_column(ForeignKey("lockers.id"))
    locker: Mapped["Locker"] = relationship(back_populates="student")

    courses: Mapped[list["Course"]] = relationship(
        secondary=m2m_student_course,  # many-to-many
        back_populates="students",
        order_by=asc(text("courses.id")),
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
    )

    def __repr__(self) -> str:
        return f"Course(id={self.id})"


class Locker(Base):
    __tablename__ = "lockers"

    code: Mapped[str] = mapped_column(String(16))
    student: Mapped[Student] = relationship(
        back_populates="locker"
    )  # Mapped[<model>] -> one-to-one

    def __repr__(self) -> str:
        return f"Locker(id={self.id})"


class Dummy(Base):
    __tablename__ = "dummies"

    foo = mapped_column(String(8), unique=True, nullable=True)
    bar = mapped_column(String(8), unique=True, nullable=True)
    baz = mapped_column(String(8), unique=True, nullable=True)
