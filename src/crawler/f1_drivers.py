from bs4 import BeautifulSoup
import aiohttp
import asyncio
import sys
import os
import json
import pandas as pd
import time
import logging
from urllib.parse import urljoin
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.getcwd()
sys.path.append(PROJECT_ROOT)
from src.utils.crawling_helpers import ssl_context, head, base_url, years

DATA_DIR = os.path.join(PROJECT_ROOT, "data", "f1_drivers_data")
os.makedirs(DATA_DIR, exist_ok=True)
CHECKPOINTS_DIR = os.path.join(PROJECT_ROOT, "data", "f1_checkpoints")
os.makedirs(CHECKPOINTS_DIR, exist_ok=True)

async def scrape_drivers_standing(session, year):
    """Scrape driver standings for a specific year (2025+ structure, simplified output)"""
    url = f"{base_url}/en/results/{year}/drivers"

    async with session.get(url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {url}. Status: {response.status}")
            return [], [], []

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')

        table = soup.find('table', class_='Table-module_table__cKsW2')
        headers = [th.get_text(strip=True).replace('.', '') for th in table.find('thead').find_all('th')]

        if not table:
            return [], [], []

        headers = [th.get_text(strip=True).replace('.', '') for th in table.find('thead').find_all('th')]
        # headers = ["Pos", "Driver", "Nationality", "Car", "Pts", "Year"]
        data = []
        driver_links = []

        for row in table.find('tbody').find_all('tr'):
            cols = row.find_all('td')
            if len(cols) < 5:
                continue

            # Position
            pos = cols[0].text.strip()

            # Driver Name
            driver_a = cols[1].find('a')
            name = ""
            if driver_a:
                full_text = driver_a.get_text(separator=" ", strip=True)
                name = re.sub(r'\b[A-Z]{3}\b', '', full_text)
                name = " ".join(dict.fromkeys(name.split()))
                name = re.sub(r'\s+', ' ', name.replace('\u00a0', ' ')).strip()

            # Nationality (SVG title, clean like teams)
            nationality_td = cols[2]
            nationality = nationality_td.text.strip()
            svg_title = nationality_td.find('svg')
            if svg_title:
                title_tag = svg_title.find('title')
                if title_tag:
                    nationality = title_tag.text.strip()
                    if nationality.lower().startswith("flag of "):
                        nationality = nationality[8:].strip()

            # Team Name
            team_a = cols[3].find('a')
            team_name = team_a.text.strip() if team_a else ""

            # Points
            points = cols[4].text.strip()

            # Year
            year_str = str(year)

            data.append([
                pos, name, nationality, team_name, points, year_str
            ])

            # For detailed scraping (if needed elsewhere)
            if driver_a and driver_a['href']:
                driver_href = driver_a['href']
                profile_url = urljoin(base_url, driver_href)
                driver_links.append((name, profile_url, year))

        return data, headers, driver_links

async def scrape_driver_results(session, driver_url):
    """Scrape detailed information for a specific driver (new F1.com table format)"""
    async with session.get(driver_url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {driver_url}. Status: {response.status}")
            return None, None, None

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')

        # Extract driver code from URL
        url_parts = driver_url.split('/')
        driver_code = url_parts[-2] if len(url_parts) > 2 else None

        # Get the race results table
        table = soup.find('table', class_='Table-module_table__cKsW2')
        if not table:
            print(f"No results table found for {driver_url}")
            return [], [], driver_code

        # Get headers automatically, but since format changed, we keep the old format
        # headers = []
        # for th in table.find('thead').find_all('th'):
        #     p = th.find('p')
        #     headers.append(p.text.strip() if p else th.text.strip())
        headers = [th.get_text(strip=True).replace('.', '') for th in table.find('thead').find_all('th')]

        # Get race results
        rows = table.find('tbody').find_all('tr')
        data = []

        # Extract year from URL (e.g. .../2025/drivers/...)
        year = None
        m = re.search(r'/(\d{4})/', driver_url)
        if m:
            year = m.group(1)

        for row in rows:
            cols = row.find_all('td')
            row_data = []
            for idx, col in enumerate(cols):
                # For "GRAND PRIX", get the text from the <a> tag only
                if idx == 0:
                    a = col.find('a')
                    if a:
                        grand_prix = ""
                        for content in reversed(a.contents):
                            if isinstance(content, str) and content.strip():
                                grand_prix = content.strip()
                                break
                        row_data.append(grand_prix)
                    else:
                        row_data.append(col.get_text(strip=True))
                # For "TEAM", get the text from the <a> tag if present
                elif idx == 2:
                    a = col.find('a')
                    row_data.append(a.get_text(strip=True) if a else col.get_text(strip=True))
                # For "Date", only keep "27 May" (not year)
                elif idx == 1:
                    p = col.find('p')
                    date_text = p.text.strip() if p else col.get_text(strip=True)
                    # Add the year (from your variable) to the date
                    date_with_year = f"{date_text} {year}"
                    row_data.append(date_with_year)
                else:
                    p = col.find('p')
                    row_data.append(p.text.strip() if p else col.get_text(strip=True))
            # Add year as last column
            row_data.append(year)
            data.append(row_data)

        return data, headers, driver_code
    
async def process_driver_data(session, driver_link_tuple):
    """Process a driver link to get detailed information"""
    driver_name, url = driver_link_tuple
    
    try:
        data, headers, driver_code = await scrape_driver_results(session, url)
        
        # Create a driver details dictionary with all the data
        driver_details = {
            'name': driver_name,
            'driver_code': driver_code,
            'url': url,
            'headers': headers,
            'race_results': data
        }
        
        return driver_details
    except Exception as e:
        print(f"Error processing driver {driver_name}: {e} with {url}")
        return None

async def collect_driver_links():
    """Collect all driver links across years"""
    all_driver_links = []
    headers_drivers = []
    drivers = []
    
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    async with aiohttp.ClientSession(connector=connector) as session:          
        tasks = [scrape_drivers_standing(session, year) for year in years]
        results = await asyncio.gather(*tasks)
    
        for driver_data, header_driver, driver_links in results:
            drivers.extend(driver_data)
            all_driver_links.extend([(link[0], link[1], link[2]) for link in driver_links])

            if len(headers_drivers) == 0:
                headers_drivers = header_driver
                
        # Save the drivers data to a JSON file (renamed to race_standing.json)
        drivers_data = {
            "headers": headers_drivers,
            "drivers": drivers
        }    
        with open(os.path.join(DATA_DIR, "race_standing.json"), 'w', encoding='utf-8') as f:
            json.dump(drivers_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(drivers)} driver standings to race_standing.json")
                
        return all_driver_links, headers_drivers, drivers

async def scrape_driver_profile(session, driver_name, profile_url):
    async with session.get(profile_url, headers=head) as response:
        if response.status != 200:
            print(f"Driver profile not found: {profile_url}. Status: {response.status}")
            return None, None

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')

        profile = {
            "profile_url": profile_url,
        }

        # --- Name ---
        h1 = soup.find('h1')
        if h1:
            spans = h1.find_all('span', recursive=False)
            if len(spans) == 2:
                first_name = spans[0].get_text(strip=True)
                last_name = spans[1].get_text(strip=True)
                profile["name"] = f"{first_name} {last_name}"
            else:
                profile["name"] = h1.get_text(strip=True)

        # --- Nationality ---
        nationality = ""
        p_tags = soup.find_all("p", class_="typography-module_body-xs-semibold__Fyfwn typography-module_lg_body-s-compact-semibold__cpAmk")
        for p in p_tags:
            # Check if the parent contains a <svg> with role="presentation" (the flag)
            if p.find_previous_sibling("svg", role="presentation"):
                nationality = p.get_text(strip=True)
                break
        profile["nationality"] = nationality

        # --- Image ---
        img_url = ""
        img_tag = soup.find("img", class_=lambda c: c and any(x in c for x in ["w-[222px]", "md:w-[305px]", "lg:w-[360px]"]))
        if img_tag and img_tag.get("src"):
            img_url = img_tag["src"]
        profile["image_url"] = img_url

        # --- All <dl> blocks (driver info, stats, biography) ---
        for dl in soup.find_all('dl', class_="DataGrid-module_dataGrid__Zk5Y8"):
            for div in dl.find_all('div', class_='DataGrid-module_item__cs9Zd'):
                dt = div.find('dt')
                dd = div.find('dd')
                if dt and dd:
                    key = dt.text.strip().lower().replace(' ', '_')
                    value = dd.text.strip()
                    profile[key] = value
                    
        return profile

        # This returns headers and data 
        # return list(profile.keys()), list(profile.values()) 

async def collect_current_driver_profiles(current_year=years[-1]):
    """Collect detailed profiles for current season drivers from the main drivers page"""
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # --- Get current season driver profile links from /en/drivers.html ---
        url = f"{base_url}/en/drivers.html"
        async with session.get(url, headers=head) as response:
            if response.status != 200:
                print(f"Failed to load {url}. Status: {response.status}")
                return [], []

            html = await response.text()
            soup = BeautifulSoup(html, 'lxml')
            driver_links = []
            for a in soup.find_all('a', attrs={'data-f1rd-a7s-click': 'driver_card_click'}):
                # Get the full name from the card (first and last name)
                name_parts = a.find_all('p')
                driver_name = " ".join([p.text.strip() for p in name_parts])
                href = a.get('href')
                if href:
                    profile_url = urljoin(base_url, href)
                    driver_links.append((driver_name, profile_url))

        # --- Process each driver profile ---
        driver_profiles = []

        for driver_name, driver_url in driver_links:
            profile = await scrape_driver_profile(session, driver_name, driver_url)
            if profile:
                driver_profiles.append(profile)

        # Optionally, collect all unique headers if you want
        all_headers = set()
        for profile in driver_profiles:
            all_headers.update(profile.keys())
        all_headers = list(all_headers)

        # Save as a list of dicts
        profiles_data = {
            "drivers": driver_profiles
        }

        profiles_file = os.path.join(DATA_DIR, f"{current_year}_driver_profiles.json")
        with open(profiles_file, 'w', encoding='utf-8') as f:
            json.dump(profiles_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(driver_profiles)} driver profiles to {profiles_file}")

        return all_headers

async def scrape_f1_driver_data(all_driver_links):
    """Scrape all F1 driver data organized by year"""
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    # Create a longer timeout
    timeout = aiohttp.ClientTimeout(total=60)
    
    start_time = time.time()
    
    # Group driver links by year
    driver_links_by_year = {}
    for name, url, year in all_driver_links:
        if year not in driver_links_by_year:
            driver_links_by_year[year] = []
        driver_links_by_year[year].append((name, url))
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Process driver standings checkpoints
        logger.info("Processing driver standings...")
        standings_results = []
        checkpoint_count = 0
        
        # Process each year
        for year, year_links in driver_links_by_year.items():
            # Create directory for the year
            year_dir = os.path.join(DATA_DIR, str(year))
            os.makedirs(year_dir, exist_ok=True)
            
            # print(f"Processing {len(year_links)} drivers for year {year}")
            
            # Process driver results
            driver_results = []
            results_processed = 0
            
            for i, link in enumerate(year_links):
                driver_name, url = link
                
                # Process the driver data
                result = await process_driver_data(session, link)
                
                if result:
                    driver_results.append(result)
                    
                    # Save directly to hierarchical structure
                    driver_name = result['name'].lower().replace(' ', '_')
                    driver_file = os.path.join(year_dir, f"{driver_name}.json")
                    
                    with open(driver_file, 'w', encoding='utf-8') as f:
                        json.dump(result, f, indent=2, ensure_ascii=False)
                        
                    results_processed += 1
                
                # Save checkpoint every 100 drivers or at the end
                checkpoint_file = os.path.join(CHECKPOINTS_DIR, "driver_results_latest.json")
                if (i + 1) % 100 == 0 or i == len(year_links) - 1:
                    checkpoint_count += 1
                    with open(checkpoint_file, 'w', encoding='utf-8') as f:
                        json.dump(driver_results, f, indent=2, ensure_ascii=False)
            
            # print(f"Processed {results_processed} drivers for year {year}")
    
    end_time = time.time()
    total_time = end_time - start_time
    
    logger.info(f"\nCompleted drivers data collection in {total_time:.2f} seconds")
    
    # Delete checkpoint file after successful completion
    checkpoint_file = os.path.join(CHECKPOINTS_DIR, "driver_results_latest.json")
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
        logger.info(f"Deleted checkpoint file: {checkpoint_file}")

    # # Create a summary file
    # summary = {
    #     "total_standing_entries": sum(len(links) for links in driver_links_by_year.values()),
    #     "total_driver_files": sum(item["processed_count"] for item in standings_results),
    #     "years_processed": len(driver_links_by_year),
    #     "execution_time": total_time
    # }
    
    # with open(os.path.join(DATA_DIR, "summary.json"), 'w') as f:
    #     json.dump(summary, f, indent=2)

    # Return the results
    return {
        "driver_standings": standings_results,
        "execution_time": total_time
    }


async def scrape_driver_async():
    # First collect all driver links
    collect_links = await collect_driver_links()

    # Collect detailed profiles for current season drivers
    await collect_current_driver_profiles()

    # # Then process all drivers with the collected links
    all_data = await scrape_f1_driver_data(collect_links[0])
    
    return True

def main():
    """Main entry point for the driver scraping script"""
    asyncio.run(scrape_driver_async())
    
if __name__ == "__main__":
    main()

