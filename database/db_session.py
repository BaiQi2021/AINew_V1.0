from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager
from sqlalchemy import text
from .models import Base
import config

settings = config.settings

def get_db_url():
    if settings.DB_DIALECT == "mysql":
        return f"mysql+asyncmy://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    elif settings.DB_DIALECT == "postgresql":
        return f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    else:
        raise ValueError(f"Unsupported database dialect: {settings.DB_DIALECT}")

def get_server_url_without_db():
    if settings.DB_DIALECT == "mysql":
        return f"mysql+asyncmy://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}"
    elif settings.DB_DIALECT == "postgresql":
        # Connect to default 'postgres' db
        return f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/postgres"
    else:
        raise ValueError("Unsupported dialect")

async def create_database_if_not_exists():
    """Creates the database if it doesn't exist."""
    server_url = get_server_url_without_db()
    
    if settings.DB_DIALECT == "mysql":
        engine = create_async_engine(server_url, echo=False)
        async with engine.connect() as conn:
            await conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {settings.DB_NAME}"))
        await engine.dispose()
        
    elif settings.DB_DIALECT == "postgresql":
        engine = create_async_engine(server_url, echo=False, isolation_level="AUTOCOMMIT")
        async with engine.connect() as conn:
            result = await conn.execute(
                text(f"SELECT 1 FROM pg_database WHERE datname = '{settings.DB_NAME}'")
            )
            if not result.scalar():
                await conn.execute(text(f"CREATE DATABASE {settings.DB_NAME}"))
        await engine.dispose()

async def init_db():
    """Initializes the database and creates tables."""
    await create_database_if_not_exists()
    
    engine = create_async_engine(get_db_url(), echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()

@asynccontextmanager
async def get_session() -> AsyncSession:
    """Provides a transactional scope around a series of operations."""
    engine = create_async_engine(get_db_url(), echo=False)
    AsyncSessionFactory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = AsyncSessionFactory()
    try:
        yield session
        await session.commit()
    except Exception as e:
        await session.rollback()
        raise e
    finally:
        await session.close()
        await engine.dispose()

