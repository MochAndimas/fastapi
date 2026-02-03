from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from app.core.config import settings

# Create a sqlite engine
sqlite_engine = create_async_engine(
    settings.SQLITE_DB_URL,
    echo=False,  # Log SQL queries in development
    poolclass=StaticPool,
    pool_pre_ping=True
)

# Create a sqlite async session
sqlite_async_session = sessionmaker(
    bind=sqlite_engine,
    expire_on_commit=False,
    class_=AsyncSession  # Define the session as AsyncSession
)

# Create a global async engine (shared across requests)
engine = create_async_engine(
    settings.DB_URL,
    echo=False,  # Log SQL queries in development
    pool_size=12,
    max_overflow=12,
    pool_timeout=500,
    pool_recycle=1800,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 10} # Set connection timeout for robustness
)

# Configure a sessionmaker that creates read-only sessions
async_session_maker = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,       # Disable autoflush (since it's read-only)
    autocommit=False,     # Disable autocommit (important for read-only)
)

async def get_db():
    """Dependency function to provide an async database session per request."""
    async with async_session_maker() as session:
        try:
            yield session
        finally:
            await session.close()

async def get_sqlite():
    """Dependency function to provide an async sqlite session per request."""
    async with sqlite_async_session() as session:
        try:
            yield session
        finally:
            await session.close()
