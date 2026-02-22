import asyncio
from backend_main_fastapi import database

async def main():
    await database.init_db_pool()
    async with database.get_db_connection() as conn:
        async with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
                    title text,
                    content text,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                ALTER TABLE documents
                    ADD COLUMN textsearchable_index_col tsvector
                    GENERATED ALWAYS AS (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(content, ''))) STORED;
                CREATE INDEX textsearch_idx ON documents USING GIN (textsearchable_index_col);
                """)
            conn.commit()

if __name__ == "__main__":
    asyncio.run(main())