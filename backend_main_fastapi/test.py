import asyncio

async def worker(sem,i):
    async with sem :
        await asyncio.sleep(2)
        print(i)

async def main():
    sem = asyncio.Semaphore(5)
    tasks = [worker(sem,i) for i in range(0,10)]
    result = await asyncio.gather(*tasks)
    

asyncio.run(main())