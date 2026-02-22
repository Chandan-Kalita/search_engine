from contextlib import asynccontextmanager
from fastapi import FastAPI
from backend_main_fastapi.database import init_db_pool, close_db_pool, get_db_connection
import importlib

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
async def get_documents(q:str, page:int):
    normalized_query = normalize_query(q);
    print(f"normalized_query: '{normalized_query}'")
    """Example endpoint to fetch documents from database."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                                    INSERT INTO search_query (query, search_count) VALUES (%s,1) ON CONFLICT (query) DO UPDATE SET search_count = search_query.search_count+1
                            """, (normalized_query))
            await cur.execute("""
                              WITH query AS (
                                    SELECT websearch_to_tsquery('english', %s) AS q,
                                        %s::text AS raw_query
                                )
                                SELECT
                                    id,
                                    title,
                                    url,
                                    ts_headline('english', content, query.q),

                                    ts_rank_cd(textsearchable_index_col, query.q) AS base_rank,

                                    ts_rank_cd(
                                        setweight(to_tsvector('english', coalesce(title,'')), 'A'),
                                        query.q
                                    ) AS title_rank,

                                    (
                                        ts_rank_cd(textsearchable_index_col, query.q)
                                        + 10.0 * ts_rank_cd(
                                            setweight(to_tsvector('english', coalesce(title,'')), 'A'),
                                            query.q
                                        )
                                        + CASE
                                            WHEN lower(title) LIKE '%%' || lower(query.raw_query) || '%%'
                                            THEN 5.0
                                            ELSE 0
                                        END
                                    ) AS final_rank

                                FROM documents, query

                                WHERE textsearchable_index_col @@ query.q

                                ORDER BY final_rank DESC

                                OFFSET %s LIMIT 10;""", (normalized_query,normalized_query, (max(page,1)-1)*10))
            documents = await cur.fetchall()
            return {"documents": documents}
        
@app.get("/autocomplete")
async def autocomplete(q:str):
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT * FROM search_query WHERE query LIKE %s ORDER BY search_count DESC LIMIT 8
            """, (f"{normalize_query(q)}%",))
            rows = await cur.fetchall()
            return rows

@app.get("/run-migration")
async def run_migration(name:str):
    migration = importlib.import_module(f"backend_main_fastapi.migrations.{name}")
    await migration.main()


def normalize_query(q:str)-> str:
    unwanted_chars = [
        '-',
        '_',
        '.',
        '/',
        '\\',
        ',',
        ':',
        ';',
        '(',
        ')',
        '[',
        ']',
        '{'
        '}',]
    new_q = q
    for chr in unwanted_chars:
        q_list = new_q.split(chr)
        new_q = " ".join(q_list)
    new_q_list = []
    for word in new_q.split(' '):
        if word:
            new_q_list.append(word.strip())
    return ' '.join(new_q_list).lower()
