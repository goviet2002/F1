import asyncio
import time
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import os
import sys
import aiohttp
PROJECT_ROOT = os.getcwd()
sys.path.append(PROJECT_ROOT)
from src.crawler.f1_race import collect_race_links, scrape_race_sessions_batch, scrape_races_year
from src.utils.crawling_helpers import ssl_context

CONCURRENCY = 8 # Adjust as needed

timeout_count = 0  # Global counter for timeouts

async def scrape_one(browser, name, url):
    global timeout_count
    print(f"\n[{time.strftime('%X')}] Scraping sessions for: {name} ({url})")
    t2 = time.time()
    try:
        sessions = await scrape_race_sessions_batch(browser, url)
    except PlaywrightTimeoutError:
        print(f"  Timeout scraping {url}")
        timeout_count += 1
        sessions = []
    t3 = time.time()
    print(f"  Sessions found: {len(sessions)} (in {t3 - t2:.2f} seconds)")
    for s_name, s_url in sessions:
        print(f"    - {s_name}: {s_url}")

async def main():
    global timeout_count
    start = time.time()
    print(f"Started at {time.strftime('%X')}")
    
    # Get all race links
    t0 = time.time()
    all_race_links, _, _ = await collect_race_links()
    t1 = time.time()
    print(f"Total races found: {len(all_race_links)} (collected in {t1 - t0:.2f} seconds)")
    
    test_links = all_race_links
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        sem = asyncio.Semaphore(CONCURRENCY)
        async def sem_scrape(name, url):
            async with sem:
                await scrape_one(browser, name, url)
        tasks = [sem_scrape(name, url) for name, url in test_links]
        await asyncio.gather(*tasks)
        await browser.close()
    end = time.time()
    print(f"\nFinished at {time.strftime('%X')}, total elapsed: {end - start:.2f} seconds")
    print(f"Total timeouts: {timeout_count}")

async def a():
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        results = await scrape_races_year(session, 2025)
        return results
        
if __name__ == "__main__":
    asyncio.run(main())
    # Uncomment the line below to test the