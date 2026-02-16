import asyncio
from database import get_db_connection
MAX_RUN=30

async def get_one_url_from_queue():
     async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM url_queue where status = 'PENDING' LIMIT 1")
            row = await cur.fetchone()
            return row

async def main():
    res = await get_one_url_from_queue()
    print(res);
    return
    for i in range(0,MAX_RUN):
        pass

if __name__ == "__main__":
    asyncio.run(main())