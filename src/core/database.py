from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import StaticPool

from core.config import settings


def get_databases_url():
    if not settings.TESTING:
        return (f"postgresql+asyncpg://{settings.POSTGRES_USER}:"
                f"{settings.POSTGRES_PASSWORD}@{settings.DB_HOST}:"
                f"{settings.DB_PORT}/{settings.POSTGRES_DB}")
    return "sqlite+aiosqlite:///:memory:"


def create_engine_with_config():
    database_url = get_databases_url()

    if settings.TESTING:
        return create_async_engine(
            url=database_url,
            echo=False,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool
        )
    else:
        return create_async_engine(url=database_url, echo=False)


engine = create_engine_with_config()
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
