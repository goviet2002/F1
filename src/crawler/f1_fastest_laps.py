from bs4 import BeautifulSoup
import aiohttp
import asyncio
import os
import json
import time
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.path import get_project_root
from utils.crawling_helpers import ssl_context, head, base_url, years, test_function

PROJECT_ROOT = get_project_root()
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "f1_fastest_laps")
os.makedirs(DATA_DIR, exist_ok=True)
CHECKPOINTS_DIR = os.path.join(PROJECT_ROOT, "data", "f1_checkpoints")
os.makedirs(CHECKPOINTS_DIR, exist_ok=True)

async def scrape_fastest_laps(session, year):
    """Scrape fastest lap data for a specific year"""
    url = f"{base_url}/en/results/{year}/fastest-laps"
    
    async with session.get(url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {url}. Status: {response.status}")
            return None

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
        
        # Find table
        table = soup.find('table', class_='f1-table-with-data')
        
        if not table:
            print(f"No fastest lap data found for {year}")
            return None
            
        # Get headers
        headers = [header.text.strip() for header in table.find('thead').find_all('th')]
        
        # Get rows
        rows = table.find('tbody').find_all('tr')
        data = []
        
        for row in rows:
            cols = row.find_all('td')
            row_data = []
            
            # Extract Grand Prix name
            grand_prix = cols[0].text.strip()
            row_data.append(grand_prix)
            
            # Extract Driver name (handle responsive design spans)
            driver_cell = cols[1]
            first_name_span = driver_cell.select_one('span.max-desktop\\:hidden')
            last_name_span = driver_cell.select_one('span.max-tablet\\:hidden')
            
            if first_name_span and last_name_span:
                first_name = first_name_span.text.strip()
                last_name = last_name_span.text.strip()
                driver_name = f"{first_name} {last_name}"
            else:
                # For older pages without responsive spans
                driver_name = driver_cell.text.strip()
                
            row_data.append(driver_name)
            
            # Extract Car/Team name
            car = cols[2].text.strip()
            row_data.append(car)
            
            # Extract Time
            time = cols[3].text.strip()
            row_data.append(time)
                
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
            print(f"Fetching fastest lap data for {year}...")
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
                
                print(f"Added {len(year_data['data'])} entries from {year}")
                
                # Save checkpoint at intervals
                checkpoint_file = os.path.join(CHECKPOINTS_DIR, "fastest_laps_latest.json")
                if (i + 1) % 5 == 0 or i == end_year - start_year:
                    with open(checkpoint_file, 'w', encoding='utf-8') as f:
                        json.dump(all_data_by_year, f, indent=2, ensure_ascii=False)
                    
                    print(f"Saved checkpoint after processing {year}")
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
        print(f"\nCompleted fastest laps data collection in {total_time:.2f} seconds")
        print(f"Total entries collected: {len(combined_data)}")
        print(f"All data saved to: {combined_file_path}")
        
        # Delete checkpoint file after successful completion
        checkpoint_file = os.path.join(CHECKPOINTS_DIR, "fastest_laps_latest.json")
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            print(f"Deleted checkpoint file: {checkpoint_file}")
        
        return {
            "headers": all_headers,
            "data": combined_data
        }

def main():
    # Collect fastest lap data
    asyncio.run(collect_fastest_laps_data())

if __name__ == "__main__":
    main()
