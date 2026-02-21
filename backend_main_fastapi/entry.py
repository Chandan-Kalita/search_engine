import asyncio
from database import get_db_connection, init_db_pool
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urlsplit, urljoin, urldefrag
MAX_CONCURRENT_WORKERS = 5  # Stay well within NeonDB free tier limit

class MyLogger:
    name:str
    def __init__(self, name):
        self.name=name
    def error(self, *message:str):
        print(self.name, " ERROR "," ".join([str(msg) for msg in message]))
    def debug(self, *message:str):
        print(self.name, " debug "," ".join([str(msg) for msg in message]))

async def get_one_url_from_queue(conn, logger:MyLogger):
    async with conn.cursor() as cur:
        try:
            await cur.execute("""
                UPDATE url_queue SET status = 'IN_PROGRESS'
                WHERE url = (
                    SELECT url FROM url_queue 
                    WHERE status = 'PENDING' 
                    LIMIT 1 
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING url
            """)
            row = await cur.fetchone()
            await conn.commit()
            if row:
                url = row["url"]
                logger.debug("Crawling :", url)
                return url
            return None
        except Exception as e :
            await conn.rollback()
            logger.error("Error while  fetching target url", e)

async def add_links_to_queue(conn, links, logger):
    if not links:
        return
    insert_query = """
        INSERT INTO url_queue (url, status) 
        VALUES (%s, %s)
        ON CONFLICT (url) DO NOTHING
    """
    values = [(link, 'PENDING') for link in links]
    async with conn.cursor() as cur:
        try:
            await cur.executemany(insert_query, values)
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            logger.error(f"Failed to insert {len(links)} URLs into queue: {e}")
            raise


async def insert_document(conn, title, text, url, logger):
    async with conn.cursor() as cur:
        try:
            await cur.execute(
                "INSERT INTO documents (title, content, url) VALUES (%s, %s, %s)",
                (title, text, url),
            )
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            logger.error(f"Failed to insert document for {url}: {e}")


async def mark_crawled(conn, url, logger):
    async with conn.cursor() as cur:
        try:
            await cur.execute(
                "UPDATE url_queue SET status='COMPLETED' WHERE url=%s",
                (url,)
            )
            await conn.commit()
            logger.debug(f"Marked as crawled: {url}")
        except Exception as e:
            await conn.rollback()
            logger.error(f"Failed to mark {url} as crawled: {e}")

async def mark_failed_with_retry(conn, url, fail_reason,logger):
    async with conn.cursor() as cur:
        try:
            await cur.execute(
                "UPDATE url_queue SET status= CASE " \
                    "WHEN retry_count <3 THEN 'PENDING' " \
                    "WHEN retry_count >= 3 THEN 'FAILED' " \
                    "END, " \
                    "retry_count=retry_count+1, " \
                    "fail_reason=%s " \
                "WHERE url=%s",
                (fail_reason, url)
            )
            await conn.commit()
            logger.debug(f"Marked as failed: {url}")
        except Exception as e:
            await conn.rollback()
            logger.error(f"Failed to mark {url} as failed: {e}")

async def get_response_from_url(url, logger, client):
    try:
        res = await client.get(url, follow_redirects=True)
        
        res.raise_for_status()  
        return {"data":res.text, "success":True}
    except httpx.HTTPStatusError as e:
        return {"data":f"HTTP error {e.response.status_code}: {e.response.text}", "success":False}
    except httpx.TimeoutException:
        return {"data":f"Error: Request timed out", "success":False}
    except httpx.ConnectError:
        return {"data":f"Error: Connection failed", "success":False}
    except httpx.RequestError as e:
        return {"data":f"Request error: {str(e)}", "success":False}
    except Exception as e:
        return {"data":f"Unexpected error: {str(e)}", "success":False}


def extract_page_info(content: str):
    soup = BeautifulSoup(content, features="html.parser")
    links = []
    for a in soup.find_all('a'):
        href = a.get('href')
        if href:
            links.append(href)
    text = soup.get_text(" ", strip=True)
    title = soup.title.string if soup.title else None
    return links, text, title

def arbitary_validation(url:str): # Return True if validation pass.
    ignore_paths = ["/de/","/es/","/fr/","/ja/", "/ko/","/pt/","/ru/","/tr/","/uk/","/zh/","/zh-hant/"]
    for i in ignore_paths:
        if url.find(i) > -1:
            return False
    return True


def filter_links(links, main_url_str):
    main_url = urlsplit(main_url_str)
    filtered_links = []
    accepted_scheme = ['http', 'https']
    accepted_hosts = [main_url.hostname]
    ignore_endings = ('.pdf', '.jpg', '.png', '.css', '.js', '.zip', '.ico')

    for link in links:
        absolute_new_url:str = urljoin(main_url_str, link)
        absolute_new_url, _ = urldefrag(absolute_new_url)
        link_obj = urlsplit(absolute_new_url)
        if (
            link_obj.scheme in accepted_scheme
            and link_obj.hostname in accepted_hosts
            and not absolute_new_url.endswith(ignore_endings)
            and arbitary_validation(absolute_new_url)
        ):
            filtered_links.append(absolute_new_url.rstrip('/'))

    return filtered_links



async def worker(semaphore:asyncio.Semaphore, worker_no):

    logger = MyLogger(f"[Worker-{worker_no}] ")
    async with httpx.AsyncClient(timeout=10) as httpxClient:
        while True:
            print('.')
            async with semaphore:
                # Single connection reused for all DB operations in this worker
                async with get_db_connection() as conn:
                    url = await get_one_url_from_queue(conn, logger)
            if url is None:
                await asyncio.sleep(100)
                continue

            response = await get_response_from_url(url, logger, httpxClient)
            if response["success"] is False:
                async with get_db_connection() as conn:
                    await mark_failed_with_retry(conn, url, response["data"], logger)
                    continue

            loop = asyncio.get_running_loop()
            links, text, title = await loop.run_in_executor(
                None, extract_page_info, response["data"]
            )
            filtered_links = await loop.run_in_executor(
                None, filter_links, links, url
            )
            async with get_db_connection() as conn:
                await add_links_to_queue(conn, filtered_links, logger)
                await insert_document(conn, title, text, url, logger)
                await mark_crawled(conn, url, logger)



async def main():
    await init_db_pool()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_WORKERS)

    
    tasks = [worker(semaphore,i) for i in range(MAX_CONCURRENT_WORKERS)]
    await asyncio.gather(*tasks)

        # if all(r is None for r in results):
        #     print("Queue exhausted, crawling complete.")
        #     break


if __name__ == "__main__":
    asyncio.run(main())