from bs4 import BeautifulSoup
import aiohttp
import asyncio
import os
import json
import time
import unicodedata
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.path import get_project_root
from utils.crawling_helpers import ssl_context, head, base_url, years, test_function

PROJECT_ROOT = get_project_root()
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "f1_race_data")
os.makedirs(DATA_DIR, exist_ok=True)
CHECKPOINTS_DIR = os.path.join(PROJECT_ROOT, "data", "f1_checkpoints")
os.makedirs(CHECKPOINTS_DIR, exist_ok=True)

async def scrape_races_year(session, year):
    # URL of the page
    url = f"{base_url}/en/results/{year}/races"

    # Send a GET request to the URL
    async with session.get(url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {url}. Status: {response.status_code}")
            return []

        # Parse the HTML content using BeautifulSoup
        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
        
        # Find table
        table = soup.find('table', class_='f1-table-with-data')
        
        if table:
            headers = [header.text.strip() for header in table.find('thead').find_all('th')]
            
            rows = table.find('tbody').find_all('tr')
            data = []
            race_links = []
            
            for row in rows:
                cols = row.find_all('td')
                row_data = []
                
                for i, col in enumerate(cols):
                    if i == 2: #Driver column
                        winner = col.text.strip().replace("\xa0", " ")[:-3]
                        row_data.append(winner)
                    else:
                        row_data.append(col.text.strip())
                        
                # Append the row data to the data list (only once per row)
                data.append(row_data)
                
                # Extract race link
                race_link = cols[0].find('a')['href']
                full_link = f"{base_url}/en/results/{year}/{race_link}"
                race_links.append((row_data[0], full_link))
                
        return data, headers, race_links

async def scrape_race_location(session, race_url):
    async with session.get(race_url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {race_url}. Status: {response.status_code}")
            return []
        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
        
        # Find the location table
        header_section = soup.find('div', class_='max-tablet:flex-col flex gap-xs')
        
        if header_section:
            location_info = header_section.find_all('p')
            
            race_date = location_info[0].text.strip()
            track = location_info[1].text.strip().split(", ")
            circuit = track[0]
            city = track[1]
            
        return race_date, circuit, city
    
def standardize_folder_name(name):
    """Convert any name to a consistent folder name format"""
    # Normalize Unicode characters
    folder_name = unicodedata.normalize('NFKD', name)
    # Remove non-ASCII characters
    folder_name = ''.join([c for c in folder_name if not unicodedata.combining(c)])
    # Convert to lowercase
    folder_name = folder_name.lower()
    # Replace special characters
    folder_name = folder_name.replace("'", "")
    folder_name = folder_name.replace("-", "_")
    folder_name = folder_name.replace(" ", "_")
    return folder_name

async def process_race_location(session, race_link_tuple):
    grand_prix, url = race_link_tuple
    year = url.split('/results/')[1].split('/')[0]

    try:
        result = await scrape_race_location(session, url)
        race_date, circuit, city = result
        return [grand_prix, circuit, city, year, race_date]
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return None

# Get available sessions for a race
async def scrape_race_sessions(session, race_url):
    async with session.get(race_url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {race_url}. Status: {response.status_code}")
            return []

        # Parse the HTML content using BeautifulSoup
        html = await response.text()   
        soup = BeautifulSoup(html, 'lxml')
        session_items = soup.find_all("ul", class_="f1-sidebar-wrapper")
        li = session_items[0].find_all("li")
        sessions = []
        
        for item in li:
                link = item.find("a")
                if link:
                    session_name = link.text.strip()
                    session_link = f"{base_url}{link['href']}"
                    sessions.append((session_name, session_link))
        if not sessions:
            None

        return sessions

async def scrape_race_results(session, session_url, session_name=None):
    async with session.get(session_url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {session_url}. Status: {response.status_code}")
            return []
        
        # Parse the HTML content using BeautifulSoup
        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
            
        # Find the location table
        table = soup.find('table', class_='f1-table-with-data')
        
        if not table:
            print(f"No table found for {session_url}")
            return None
        
        headers = [header.text.strip() for header in table.find('thead').find_all('th')]
        rows = table.find('tbody').find_all('tr')
        data = []
        
        for row in rows:
            cols = row.find_all('td')
            row_data = []
            
            for i, col in enumerate(cols):
                if i == 2: #Driver column
                    winner = col.text.strip().replace("\xa0", " ")[:-3]
                    row_data.append(winner)
                else:
                    row_data.append(col.text.strip())
                    
            data.append(row_data)
        return headers, data, session_url, session_name

headers_race_location = ['Grand Prix', 'Circuit', 'Country/City', 'Year', 'Date']
race_location = []

# Collect all race links
async def collect_race_links():
    all_race_links = []
    headers_race = []
    races = []
    
    connector = aiohttp.TCPConnector(ssl=ssl_context)

    
    async with aiohttp.ClientSession(connector=connector) as session:          
        tasks = [scrape_races_year(session, year) for year in years]
        results = await asyncio.gather(*tasks)
    
        for race, header_race, race_links in results:
            races.extend(race)
            all_race_links.extend([(link[0], link[1]) for link in race_links])

            if len(headers_race) == 0:
                headers_race = header_race
                
        # Save the races data to a JSON file
        races_data = {
            "headers": headers_race,
            "races": races
        }    
        with open(os.path.join(DATA_DIR, "races.json"), 'w', encoding='utf-8') as f:
            json.dump(races_data, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(races)} races to all_races.json")
                
        return all_race_links, headers_race, races

async def scrape_f1_data_with_checkpoints(all_race_links):
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    # Create a longer timeout
    timeout = aiohttp.ClientTimeout(total=60)
    
    start_time = time.time()
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Process Race Location concurrently with incremental saves
        print("Processing race locations...")
        location_results = []
        checkpoint_count = 0
        
        for i, link in enumerate(all_race_links):
            result = await process_race_location(session, link)
            
            if result:  # Only process valid results
                location_results.append(result)
                
                # Save directly to hierarchical structure
                grand_prix, circuit, city, year, date = result
                
                # Use standardized folder naming
                gp_name = standardize_folder_name(grand_prix)
                race_dir = os.path.join(DATA_DIR, str(year), gp_name)
                os.makedirs(race_dir, exist_ok=True)
                
                # Save race metadata
                metadata = {
                    "grand_prix": grand_prix,
                    "circuit": circuit,
                    "city": city, 
                    "year": year,
                    "date": date
                }
                
                with open(os.path.join(race_dir, "race_metadata.json"), 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            # Save checkpoint every 100 races or at the end
            checkpoint_file =  os.path.join(CHECKPOINTS_DIR, "race_locations_latest.json")
            if (i + 1) % 1000 == 0 or i == len(all_race_links) - 1:
                checkpoint_count += 1
                with open(checkpoint_file, 'w', encoding='utf-8') as f:
                    json.dump(location_results, f, indent=2, ensure_ascii=False)

        print(f"Processed {len(location_results)} race locations")
        

        
        # Process Race Sessions with incremental saves
        print("Getting race sessions...")
        session_results = []
        all_sessions = []
        checkpoint_count = 0
        
        for i, link in enumerate(all_race_links):
            sessions = await scrape_race_sessions(session, link[1])
            
            if sessions:
                session_results.append(sessions)
                all_sessions.extend(sessions)
                
            # Save checkpoint every 100 races or at the end
            checkpoint_file =  os.path.join(CHECKPOINTS_DIR, "race_sessions_latest.json")
            if (i + 1) % 1000 == 0 or i == len(all_race_links) - 1:
                checkpoint_count += 1
                with open(checkpoint_file, 'w', encoding='utf-8') as f:
                    json.dump(session_results, f, indent=2, ensure_ascii=False)

        print(f"Found {len(all_sessions)} total session results to process")

        # Scrape Race Results with incremental saves to hierarchical structure
        print("Processing race results...")
        race_result = {}
        checkpoint_count = 0
        results_processed = 0
        
        for i, task in enumerate(all_sessions):
            result = await scrape_race_results(session, task[1], task[0])
            
            if result is not None:
                headers, data, url, session_name = result
                race_result[url] = {
                    "header": headers,
                    "data": data,
                    "session_name": session_name
                }
                
                # Save directly to hierarchical structure
                parts = url.split('/')
                year = parts[5]
                
                # Extract race name from URL
                race_location = parts[8] if len(parts) > 8 else "unknown"
                session_type = session_name.lower().replace(' ', '-').replace('-', '_')

                # Use the standardized folder name function
                race_location = standardize_folder_name(race_location)
                race_dir = os.path.join(DATA_DIR, str(year), race_location)
                os.makedirs(race_dir, exist_ok=True)
                
                # Save session data
                session_filename = f"{session_type}.json"
                with open(os.path.join(race_dir, session_filename), 'w', encoding='utf-8') as f:
                    json.dump({
                        "header": headers,
                        "data": data,
                        "session_name": session_name
                    }, f, indent=2, ensure_ascii=False)
                    
                results_processed += 1
                
            # Save checkpoint every 200 sessions or at the end
            checkpoint_file =  os.path.join(CHECKPOINTS_DIR, "race_results_latest.json")
            if (i + 1) % 1000 == 0 or i == len(all_sessions) - 1:
                checkpoint_count += 1
                with open(checkpoint_file, 'w', encoding='utf-8') as f:
                    json.dump(race_result, f, indent=2, ensure_ascii=False)
                
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"Processed {results_processed} race results")
        print(f"Total execution time: {total_time:.2f} seconds")
        
        # Delete checkpoint file after successful completion
        checkpoint_files = [
            os.path.join(CHECKPOINTS_DIR, "race_locations_latest.json"),
            os.path.join(CHECKPOINTS_DIR, "race_sessions_latest.json"),
            os.path.join(CHECKPOINTS_DIR, "race_results_latest.json")
        ]
        
        for checkpoint_file in checkpoint_files:
            if os.path.exists(checkpoint_file):
                os.remove(checkpoint_file)
                print(f"Deleted checkpoint file: {checkpoint_file}")

        # # Create a summary file
        # summary = {
        #     "total_races": len(location_results),
        #     "total_sessions": len(all_sessions),
        #     "total_results": results_processed,
        #     "execution_time": total_time
        # }
        
        # with open(os.path.join(DATA_DIR, "summary.json"), 'w') as f:
        #     json.dump(summary, f, indent=2)

        # Return the results
        return {
            "race_location": location_results,
            "race_sessions": all_sessions,
            "race_result": race_result,
            "execution_time": total_time
        }
        
def main():
    collect_links = asyncio.run(collect_race_links())
    all_data = asyncio.run(scrape_f1_data_with_checkpoints(collect_links[0]))

if __name__ == "__main__":
    main()
