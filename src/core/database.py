from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from core.config import settings


DATABASE_URL = (f"postgresql+asyncpg://{settings.POSTGRES_USER}:"
                f"{settings.POSTGRES_PASSWORD}@{settings.DB_HOST}:"
                f"{settings.DB_PORT}/{settings.POSTGRES_DB}")

engine = create_async_engine(url=DATABASE_URL, echo=True)
async_session = async_sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


async def create_database():
    async with engine.connect() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()


async def get_session():
    async with async_session() as session:
        yield session
