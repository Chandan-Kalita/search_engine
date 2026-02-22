import asyncio
from backend_main_fastapi import database

async def main():
    await database.init_db_pool()
    async with database.get_db_connection() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS search_query (
                        query text PRIMARY KEY,
                        search_count int default 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE INDEX search_query_search_count_idx
                    ON search_query (search_count DESC);
                    """)
                await conn.commit()
                print("Migration ran successfully")
            except Exception as e:
                print("Failed to run migration. reverting back...")
                await conn.rollback()

if __name__ == "__main__":
    asyncio.run(main())