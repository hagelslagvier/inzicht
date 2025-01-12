from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import Engine
from sqlalchemy.orm import Session


@contextmanager
def session_factory(bind: Engine) -> Generator[Session, None, None]:
    with Session(bind=bind, expire_on_commit=False) as session:
        try:
            session.begin()
            yield session
        except:
            session.rollback()
            raise
        else:
            session.commit()
