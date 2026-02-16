from contextlib import asynccontextmanager
from dotenv import load_dotenv
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row
import os

load_dotenv()  # Load environment variables from .env file
# Database connection string
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@localhost:5432/dbname"
)

# Connection pool (will be initialized on app startup)
pool: AsyncConnectionPool | None = None


def get_pool() -> AsyncConnectionPool:
    """Get the database connection pool."""
    if pool is None:
        raise RuntimeError("Database pool not initialized")
    return pool


async def init_db_pool():
    """Initialize the database connection pool."""
    global pool
    pool = AsyncConnectionPool(
        conninfo=DATABASE_URL,
        open=False,  # Explicit deferred initialization (best practice)
        min_size=2,
        max_size=10,
        kwargs={"row_factory": dict_row}
    )
    await pool.open()
    print("Database pool initialized")


async def close_db_pool():
    """Close the database connection pool."""
    global pool
    if pool:
        await pool.close()
        pool = None
        print("Database pool closed")


@asynccontextmanager
async def get_db_connection():
    """Get a database connection from the pool."""
    pool_instance = get_pool()
    async with pool_instance.connection() as conn:
        yield conn
