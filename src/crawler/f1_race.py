from bs4 import BeautifulSoup
import aiohttp
import asyncio
import os
import json
import time
import sys
import logging
from playwright.async_api import async_playwright
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.getcwd()
sys.path.append(PROJECT_ROOT)
from src.utils.crawling_helpers import ssl_context, head, base_url, years, standardize_folder_name

DATA_DIR = os.path.join(PROJECT_ROOT, "data", "f1_race_data")
os.makedirs(DATA_DIR, exist_ok=True)
CHECKPOINTS_DIR = os.path.join(PROJECT_ROOT, "data", "f1_checkpoints")
os.makedirs(CHECKPOINTS_DIR, exist_ok=True)

async def scrape_races_year(session, year):
    url = f"{base_url}/en/results/{year}/races"

    async with session.get(url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {url}. Status: {response.status}")
            return []

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')

        table = soup.find('table', class_='f1-table-with-data')
        if not table:
            print(f"No race table found for {year}")
            return [], [], []

        # Extract headers from <p> inside <th>
        # headers = [th.p.text.strip() for th in table.find('thead').find_all('th')]
        headers = ['Grand Prix', 'Date', 'Winner', 'Car', 'Laps', 'Time']

        rows = table.find('tbody').find_all('tr')
        data = []
        race_links = []

        for row in rows:
            cols = row.find_all('td')
            row_data = []

            # 1. Grand Prix name (after SVG in <a>)
            gp_cell = cols[0]
            a_tag = gp_cell.find('a')
            if a_tag:
                svg = a_tag.find('svg')
                if svg and svg.next_sibling:
                    gp_name = svg.next_sibling.strip()
                else:
                    gp_name = a_tag.get_text(strip=True)
                row_data.append(gp_name)
                # Race link
                race_href = a_tag.get('href', '')
                # Clean up relative URL
                if race_href.startswith("/"):
                    full_link = base_url.rstrip("/") + race_href
                else:
                    full_link = base_url.rstrip("/") + "/" + race_href
                race_links.append((gp_name, full_link))
            else:
                row_data.append(gp_cell.text.strip())
                race_links.append((gp_cell.text.strip(), ""))

            # 2. Date
            date_cell = cols[1]
            date_val = date_cell.find('p').text.strip() if date_cell.find('p') else date_cell.text.strip()
            row_data.append(date_val)

            # 3. Winner (from <span class="test">)
            winner_cell = cols[2]
            winner_name = ""
            test_span = winner_cell.find('span', class_='test')
            if test_span:
                first_name = test_span.find('span', class_='max-lg:hidden')
                last_name = test_span.find('span', class_='max-md:hidden')
                if first_name and last_name:
                    winner_name = f"{first_name.text.strip()} {last_name.text.strip()}"
                else:
                    winner_name = test_span.get_text(strip=True)
            else:
                winner_name = winner_cell.text.strip()
            row_data.append(winner_name)

            # 4. Team (from <span> after logo)
            team_cell = cols[3]
            team_name = ""
            team_span = team_cell.find('span', class_='flex')
            if team_span:
                logo_span = team_span.find('span', class_='TeamLogo-module_teamlogo__lA3j1')
                if logo_span and logo_span.next_sibling:
                    team_name = logo_span.next_sibling.strip()
                else:
                    team_name = team_span.get_text(strip=True)
            else:
                team_name = team_cell.text.strip()
            row_data.append(team_name)

            # 5. Laps
            laps_cell = cols[4]
            laps_val = laps_cell.find('p').text.strip() if laps_cell.find('p') else laps_cell.text.strip()
            row_data.append(laps_val)

            # 6. Time
            time_cell = cols[5]
            time_val = time_cell.find('p').text.strip() if time_cell.find('p') else time_cell.text.strip()
            row_data.append(time_val)

            data.append(row_data)

        return data, headers, race_links

async def scrape_race_location(session, race_url):
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
async def scrape_race_sessions(race_url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_viewport_size({"width": 1300, "height": 800})
        await page.goto(race_url, wait_until="domcontentloaded")

        await page.wait_for_selector('#sidebar-dropdown')
        await page.click('#sidebar-dropdown')
        
        parsed = urlparse(race_url)
        base_path = parsed.path.rsplit('/', 1)[0] + '/'
        
        # Directly extract session links with Playwright
        session_elements = await page.query_selector_all('a.DropdownMenuItem-module_dropdown-menu-item__6Y3-v')
        
        sessions = []
        for elem in session_elements:
            href = await elem.get_attribute('href')
            if href and len(href) > len("/en/results/2025/races") and href.startswith(base_path):
                # Try to get the country/session name from the nested span if present
                country_span = await elem.query_selector('span.mr-px-32')
                if country_span:
                    continue
                else:
                    session_text = await elem.inner_text()
                    session_name = session_text.replace("Active", "").strip()
                    sessions.append((session_name, f"https://www.formula1.com{href}"))

        await browser.close()
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
        
        # headers = [header.text.strip() for header in table.find('thead').find_all('th')]
        headers = [['Pos', 'No', 'Driver', 'Car', 'Time', 'Laps']]
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
        
        logger.info(f"Saved {len(races)} races to all_races.json")
                
        return all_race_links, headers_race, races

async def scrape_f1_data_with_checkpoints(all_race_links):
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    # Create a longer timeout
    timeout = aiohttp.ClientTimeout(total=60)
    
    start_time = time.time()
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Process Race Location concurrently with incremental saves
        logger.info("Processing race locations...")
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

        logger.info(f"Processed {len(location_results)} race locations")
        
        # Process Race Sessions with incremental saves
        logger.info("Getting race sessions...")
        session_results = []
        all_sessions = []
        checkpoint_count = 0
        
        for i, link in enumerate(all_race_links):
            sessions = await scrape_race_sessions(link[1])
            
            if sessions:
                session_results.append(sessions)
                all_sessions.extend(sessions)
                
            # Save checkpoint every 100 races or at the end
            checkpoint_file =  os.path.join(CHECKPOINTS_DIR, "race_sessions_latest.json")
            if (i + 1) % 1000 == 0 or i == len(all_race_links) - 1:
                checkpoint_count += 1
                with open(checkpoint_file, 'w', encoding='utf-8') as f:
                    json.dump(session_results, f, indent=2, ensure_ascii=False)

        logger.info(f"Found {len(all_sessions)} total session results to process")

        # Scrape Race Results with incremental saves to hierarchical structure
        logger.info("Processing race results...")
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
        
        logger.info(f"Processed {results_processed} race results")
        logger.info(f"\nCompleted races data collection in {total_time:.2f} seconds")
        
        # Delete checkpoint file after successful completion
        checkpoint_files = [
            os.path.join(CHECKPOINTS_DIR, "race_locations_latest.json"),
            os.path.join(CHECKPOINTS_DIR, "race_sessions_latest.json"),
            os.path.join(CHECKPOINTS_DIR, "race_results_latest.json")
        ]
        
        for checkpoint_file in checkpoint_files:
            if os.path.exists(checkpoint_file):
                os.remove(checkpoint_file)
                logger.info(f"Deleted checkpoint file: {checkpoint_file}")

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
        
async def scrape_race_async():
    collect_links = await collect_race_links()
    all_data = await scrape_f1_data_with_checkpoints(collect_links[0])
    
    return True

def main():
    asyncio.run(scrape_race_async())

if __name__ == "__main__":
    main()
