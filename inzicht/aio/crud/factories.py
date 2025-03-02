from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession


@asynccontextmanager
async def async_session_factory(
    bind: AsyncEngine,
) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(bind=bind, expire_on_commit=False) as session:
        try:
            await session.begin()
            yield session
        except:
            await session.rollback()
            raise
        else:
            await session.commit()
