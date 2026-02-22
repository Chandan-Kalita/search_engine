import asyncio
from backend_main_fastapi import database

async def main():
    print("Migration ran successfully")
    # await database.init_db_pool()
    # async with database.get_db_connection() as conn:
    #     async with conn.cursor() as cur:
    #         cur.execute("")
    #         conn.commit()

if __name__ == "__main__":
    asyncio.run(main())