from bs4 import BeautifulSoup
import aiohttp
import asyncio
import os
import json
import time
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.getcwd()
sys.path.append(PROJECT_ROOT)
from src.utils.crawling_helpers import ssl_context, head, base_url, years

DATA_DIR = os.path.join(PROJECT_ROOT, "data", "f1_fastest_laps")
os.makedirs(DATA_DIR, exist_ok=True)
CHECKPOINTS_DIR = os.path.join(PROJECT_ROOT, "data", "f1_checkpoints")
os.makedirs(CHECKPOINTS_DIR, exist_ok=True)

async def scrape_fastest_laps(session, year):
    """Scrape fastest lap data for a specific year (new 2025+ format)"""
    url = f"{base_url}/en/results/{year}/awards/fastest-laps"

    async with session.get(url, headers=head) as response:
        if response.status != 200:
            logger.info(f"Failed to load {url}. Status: {response.status}")
            return None

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')

        # Find the awards table by id
        table_wrapper = soup.find('div', id='awards-table')
        if not table_wrapper:
            print(f"No awards table found for {year}")
            return None
        table = table_wrapper.find('table')
        if not table:
            print(f"No fastest lap data found for {year}")
            return None

        # Get headers from <th>
        headers = [th.text.strip() for th in table.find('thead').find_all('th')]

        # Get rows
        rows = table.find('tbody').find_all('tr')
        data = []
        for row in rows:
            cols = row.find_all('td')
            row_data = []
            # 1. Grand Prix name
            gp_cell = cols[0]
            a_tag = gp_cell.find('a')
            if a_tag:
                # Get only the text after the SVG (the Grand Prix name)
                texts = [t for t in a_tag.stripped_strings if not t.startswith("Flag of")]
                gp_name = " ".join(texts)
                row_data.append(gp_name)
            else:
                row_data.append(gp_cell.text.strip())
            # 2. Winner name
            winner_cell = cols[1]
            first_name = winner_cell.find('span', class_='max-lg:hidden')
            last_name = winner_cell.find('span', class_='max-md:hidden')
            if first_name and last_name:
                winner = f"{first_name.text.strip()} {last_name.text.strip()}"
            else:
                winner = winner_cell.get_text(strip=True)
            row_data.append(winner)
            # 3. Time
            time_cell = cols[2]
            time_val = time_cell.get_text(strip=True)
            row_data.append(time_val)
            data.append(row_data)

        output = {
            "headers": headers,
            "data": data
        }
        return output

async def collect_fastest_laps_data(start_year=years[0], end_year=years[-1]):
    """Collect fastest lap data for a range of years into a single file with year column"""
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    timeout = aiohttp.ClientTimeout(total=60)
    start_time = time.time()
    
    # Collection to store combined data
    all_headers = None
    combined_data = []
    all_data_by_year = {}  # For checkpoints

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for i, year in enumerate(range(start_year, end_year + 1)):
            # print(f"Fetching fastest lap data for {year}...")
            year_data = await scrape_fastest_laps(session, year)
            
            if year_data:
                # Set headers if not already set
                if all_headers is None:
                    # Add "Year" to the headers list
                    all_headers = year_data["headers"] + ["Year"]
                
                # Add year column to each row and add to combined data
                for row in year_data["data"]:
                    # Add year to each row
                    row_with_year = row + [str(year)]
                    combined_data.append(row_with_year)
                
                # Store in year-indexed structure for checkpoint
                all_data_by_year[str(year)] = {
                    "headers": year_data["headers"],
                    "data": year_data["data"]
                }
                
                # print(f"Added {len(year_data['data'])} entries from {year}")
                
                # Save checkpoint at intervals
                checkpoint_file = os.path.join(CHECKPOINTS_DIR, "fastest_laps_latest.json")
                if (i + 1) % 5 == 0 or i == end_year - start_year:
                    with open(checkpoint_file, 'w', encoding='utf-8') as f:
                        json.dump(all_data_by_year, f, indent=2, ensure_ascii=False)
                    
                    # print(f"Saved checkpoint after processing {year}")
            else:
                print(f"No data available for {year}")
        
        # Final save of the combined data
        combined_file_path = os.path.join(DATA_DIR, "fastest_laps.json")
        with open(combined_file_path, 'w', encoding='utf-8') as f:
            json.dump({
                "headers": all_headers,
                "data": combined_data
            }, f, indent=2, ensure_ascii=False)
        
        end_time = time.time()
        total_time = end_time - start_time
        logger.info(f"\nCompleted fastest laps data collection in {total_time:.2f} seconds")
        logger.info(f"Total entries collected: {len(combined_data)}")
        logger.info(f"All data saved to: {combined_file_path}")
        
        # Delete checkpoint file after successful completion
        checkpoint_file = os.path.join(CHECKPOINTS_DIR, "fastest_laps_latest.json")
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            logger.info(f"Deleted checkpoint file: {checkpoint_file}")
        
        return {
            "headers": all_headers,
            "data": combined_data
        }

async def scrape_fastest_laps_async():
    # Collect fastest lap data
    await collect_fastest_laps_data()
    
    return True

def main():
    asyncio.run(scrape_fastest_laps_async())

if __name__ == "__main__":
    main()