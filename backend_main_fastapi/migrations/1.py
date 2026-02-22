import asyncio
from backend_main_fastapi import database

async def main():
    await database.init_db_pool()
    async with database.get_db_connection() as conn:
        async with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS url_queue (
                url TEXT PRIMARY KEY,
                status TEXT,
                retry_count INT DEFAULT 0, 
                fail_reason TEXT
                );
                """)
            conn.commit()

if __name__ == "__main__":
    asyncio.run(main())