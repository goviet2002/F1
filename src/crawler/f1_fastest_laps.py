from bs4 import BeautifulSoup
import aiohttp
import asyncio
import os
import json
import time
import sys
import logging

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
    url = f"{base_url}/en/results/{year}/fastest-laps"

    async with session.get(url, headers=head) as response:
        if response.status != 200:
            logger.info(f"Failed to load {url}. Status: {response.status}")
            return None

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')

        # Find table
        table = soup.find('table', class_='f1-table-with-data')
        if not table:
            print(f"No fastest lap data found for {year}")
            return None

        # Get headers (from <p> inside <th>)
        # headers = [th.p.text.strip() for th in table.find('thead').find_all('th')]
        headers = ["Grand Prix", "Driver", "Car", "Time"]
        
        # Get rows
        rows = table.find('tbody').find_all('tr')
        data = []

        for row in rows:
            cols = row.find_all('td')
            row_data = []

            # 1. Grand Prix name (from <a> text, after SVG)
            gp_cell = cols[0]
            a_tag = gp_cell.find('a')
            if a_tag:
                # The text after the SVG is the GP name
                gp_name = a_tag.get_text(strip=True)
                # Remove the flag SVG text if present
                svg = a_tag.find('svg')
                if svg and svg.next_sibling:
                    gp_name = svg.next_sibling.strip()
                row_data.append(gp_name)
            else:
                row_data.append(gp_cell.text.strip())

            # 2. Driver name (from <span class="test">)
            driver_cell = cols[1]
            driver_name = ""
            test_span = driver_cell.find('span', class_='test')
            if test_span:
                # Get all visible name spans
                first_name = test_span.find('span', class_='max-lg:hidden')
                last_name = test_span.find('span', class_='max-md:hidden')
                if first_name and last_name:
                    driver_name = f"{first_name.text.strip()} {last_name.text.strip()}"
                else:
                    driver_name = test_span.get_text(strip=True)
            else:
                driver_name = driver_cell.text.strip()
            row_data.append(driver_name)

            # 3. Team name (from <span> after logo)
            team_cell = cols[2]
            team_name = ""
            team_span = team_cell.find('span', class_='flex')
            if team_span:
                # The team name is the text after the logo <span>
                logo_span = team_span.find('span', class_='TeamLogo-module_teamlogo__lA3j1')
                if logo_span and logo_span.next_sibling:
                    team_name = logo_span.next_sibling.strip()
                else:
                    team_name = team_span.get_text(strip=True)
            else:
                team_name = team_cell.text.strip()
            row_data.append(team_name)

            # 4. Time (from <p>)
            time_cell = cols[3]
            time_val = time_cell.find('p').text.strip() if time_cell.find('p') else time_cell.text.strip()
            row_data.append(time_val)

            data.append(row_data)

        # Create output structure
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