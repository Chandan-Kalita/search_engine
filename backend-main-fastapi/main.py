from contextlib import asynccontextmanager
from fastapi import FastAPI
from database import init_db_pool, close_db_pool, get_db_connection


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize database pool
    await init_db_pool()
    yield
    # Shutdown: Close database pool
    await close_db_pool()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/db-test")
async def test_database():
    """Test database connection by querying PostgreSQL version."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT version()")
            result = await cur.fetchone()
            return {"database_version": result["version"] if result else None}


@app.get("/documents")
async def get_documents(q:str):
    """Example endpoint to fetch documents from database."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"SELECT * FROM documents WHERE title ilike '%{q}%' or content ilike '%{q}%' LIMIT 10")
            documents = await cur.fetchall()
            return {"documents": documents}