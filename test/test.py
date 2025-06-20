from bs4 import BeautifulSoup
import os
import aiohttp
import asyncio
import sys
PROJECT_ROOT = os.getcwd()
sys.path.append(PROJECT_ROOT)
from src.utils.crawling_helpers import ssl_context, head, base_url, years, standardize_folder_name

async def scrape_race_location(race_url):
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
        # Create a longer timeout
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async with session.get(race_url, headers=head) as response:
            if response.status != 200:
                print(f"Failed to load {race_url}. Status: {response.status_code}")
                return []
            html = await response.text()
            soup = BeautifulSoup(html, 'lxml')
            
            # Find the location table
            header_section = soup.find('div', class_='flex flex-col gap-px-6 text-text-3')
            
            if header_section:
                location_info = header_section.find_all('p')
                
                race_date = location_info[0].text.strip()
                track = location_info[1].text.strip().split(", ")
                circuit = track[0]
                city = track[1]
                
            return race_date, circuit, city




print(asyncio.run(scrape_race_location("https://www.formula1.com/en/results/2000/races/47/australia/race-result")))