from bs4 import BeautifulSoup
import aiohttp
import asyncio
import sys
import os
import json
import pandas as pd
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.path import get_project_root
from utils.f1_shared import ssl_context, head, base_url, years, test_function

PROJECT_ROOT = get_project_root()
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "f1_drivers_data")
os.makedirs(DATA_DIR, exist_ok=True)
CHECKPOINTS_DIR = os.path.join(PROJECT_ROOT, "data", "f1_checkpoints")

async def scrape_drivers_standing(session, year):
    """Scrape driver standings for a specific year"""
    url = f"{base_url}/en/results/{year}/drivers"
    
    async with session.get(url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {url}. Status: {response.status}")
            return [], [], []

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
        
        # Find table
        table = soup.find('table', class_='f1-table-with-data')
        
        if table:
            headers = [header.text.strip() for header in table.find('thead').find_all('th')]
            # Add YEAR to headers
            if 'Year' not in headers:
                headers.append('Year')
            
            rows = table.find('tbody').find_all('tr')
            data = []
            driver_links = []
            
            for row in rows:
                cols = row.find_all('td')
                row_data = []
                
                for i, col in enumerate(cols):
                    if i == 1:  # Driver name column
                        # Extract first and last name separately, ignoring the driver code
                        driver_a = col.find('a')
                        if driver_a:
                            first_name_span = driver_a.find('span', class_='max-desktop:hidden')
                            last_name_span = driver_a.find('span', class_='max-tablet:hidden')
                            
                            first_name = first_name_span.text.strip() if first_name_span else ""
                            last_name = last_name_span.text.strip() if last_name_span else ""
                            
                            full_name = f"{first_name} {last_name}".strip()
                            row_data.append(full_name)
                        else:
                            row_data.append(col.text.strip())
                    else:
                        row_data.append(col.text.strip())
                
                # Add year to each row
                row_data.append(str(year))
                data.append(row_data)
                
                # Extract driver link
                driver_link = cols[1].find('a')['href'] if cols[1].find('a') else None
                if driver_link:
                    # Make sure the driver_link starts with a slash if needed
                    if not driver_link.startswith('/'):
                        driver_link = f"/{driver_link}"
                        
                    # Add the full URL
                    full_link = f"{base_url}/en/results/{year}{driver_link}"
                    driver_links.append((row_data[1], full_link, year))  # Add year to link tuple
                
        return data, headers, driver_links

async def scrape_driver_results(session, driver_url):
    """Scrape detailed information for a specific driver"""
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
        table = soup.find('table', class_='f1-table-with-data')
        if not table:
            print(f"No results table found for {driver_url}")
            return [], [], driver_code
            
        # Get headers
        headers = [header.text.strip() for header in table.find('thead').find_all('th')]
        
        # Get race results
        rows = table.find('tbody').find_all('tr')
        data = []
        
        for row in rows:
            cols = row.find_all('td')
            row_data = []
            
            for col in cols:
                row_data.append(col.text.strip())
                
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
        print(f"Error processing driver {driver_name}: {e}")
        return None

async def collect_driver_links():
    """Collect all driver links across years"""
    all_driver_links = []
    headers_drivers = []
    drivers = []

    os.makedirs(DATA_DIR, exist_ok=True)
    
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
        
        print(f"Saved {len(drivers)} driver standings to race_standing.json")
                
        return all_driver_links, headers_drivers, drivers

async def scrape_driver_profile(session, driver_name, driver_code):
    """Scrape detailed profile information for a driver from the main drivers page"""
    # Extract the last name part from the driver code URL
    name_part = driver_name.lower().replace(' ', '-')
    
    # Construct profile URL
    profile_url = f"{base_url}/en/drivers/{name_part}.html"
    
    try:
        async with session.get(profile_url, headers=head) as response:
            if response.status != 200:
                print(f"Driver profile not found: {profile_url}. Status: {response.status}")
                return None
            
            html = await response.text()
            soup = BeautifulSoup(html, 'lxml')
            
            # Get the driver info section (with Team, Country, etc.)
            driver_info_section = soup.find('div', class_='f1-dl')
            
            # Initialize lists for headers and data
            headers = ["name", "driver_code", "profile_url"]
            data = [driver_name, driver_code, profile_url]
            
            if driver_info_section:
                # Extract info from dt/dd pairs
                dt_elements = driver_info_section.find_all('dt')
                dd_elements = driver_info_section.find_all('dd')
                
                for dt, dd in zip(dt_elements, dd_elements):
                    # Convert header to lowercase with underscores
                    header = dt.text.strip().lower().replace(' ', '_')
                    headers.append(header)
                    data.append(dd.text.strip())
            
            # Get biographical info (DOB, birthplace)
            bio_section = soup.find('div', class_='biography')
            if bio_section:
                bio_items = bio_section.find_all('p')
                for item in bio_items:
                    text = item.text.strip()
                    if text.startswith("Date of birth"):
                        headers.append("date_of_birth")
                        data.append(text.replace("Date of birth", "").strip())
                    elif text.startswith("Place of birth"):
                        headers.append("place_of_birth")
                        data.append(text.replace("Place of birth", "").strip())
            
            # Get driver image - try multiple approaches
            driver_img = None

            # Try main profile image first
            for img in soup.find_all('img', class_='f1-c-image'):
                if img.get('alt') and driver_name.lower() in img.get('alt').lower():
                    driver_img = img
                    break

            # Alternative approach using figure element
            if not driver_img:
                driver_figure = soup.find('figure', class_='f1-utils-flex-container')
                if driver_figure:
                    driver_img = driver_figure.find('img')

            # Extract image URL from the first approach that worked
            if driver_img:
                img_url = None
                for attr in ['src', 'data-src', 'srcset']:
                    if attr in driver_img.attrs:
                        img_url = driver_img[attr]
                        break
                        
                if img_url:
                    headers.append("image_url")
                    data.append(img_url)
            
            return headers, data
    except Exception as e:
        print(f"Error scraping profile for {driver_name}: {e}")
        return None, None

async def collect_current_driver_profiles(current_year=years[-1]):
    """Collect detailed profiles for current season drivers"""
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    timeout = aiohttp.ClientTimeout(total=60)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Get current season drivers
        current_drivers_data = await scrape_drivers_standing(session, current_year)
        drivers_data, headers, driver_links = current_drivers_data
                
        # Process each driver profile
        all_headers = []
        driver_profiles = []
        
        for driver_name, driver_url, year in driver_links:
            # Extract driver code from URL
            url_parts = driver_url.split('/')
            driver_code = url_parts[-2] if len(url_parts) > 2 else None
            
            if driver_code:
                headers, data = await scrape_driver_profile(session, driver_name, driver_code)
                if headers and data:
                    # Update all_headers to include any new fields
                    for header in headers:
                        if header not in all_headers:
                            all_headers.append(header)
                    
                    driver_profiles.append(data)
        
        # Normalize data - ensure all rows have the same number of fields
        normalized_profiles = []
        for profile in driver_profiles:
            # Create a dict from the headers and data
            profile_dict = dict(zip(headers, profile))
            
            # Create a new row with all headers
            normalized_row = []
            for header in all_headers:
                normalized_row.append(profile_dict.get(header, ""))
            
            normalized_profiles.append(normalized_row)
        
        # Save profiles to a JSON file in table format
        profiles_data = {
            "headers": all_headers,
            "drivers": normalized_profiles
        }
        
        profiles_file = os.path.join(DATA_DIR, f"{current_year}_driver_profiles.json")
        with open(profiles_file, 'w', encoding='utf-8') as f:
            json.dump(profiles_data, f, indent=2, ensure_ascii=False)
            
        print(f"Saved {len(driver_profiles)} driver profiles to {profiles_file}")
        
        return all_headers, normalized_profiles

async def scrape_f1_driver_data(all_driver_links):
    """Scrape all F1 driver data organized by year"""
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    # Create a longer timeout
    timeout = aiohttp.ClientTimeout(total=60)
    
    # Create checkpoint directory and main data directory
    os.makedirs(CHECKPOINTS_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    
    start_time = time.time()
    
    # Group driver links by year
    driver_links_by_year = {}
    for name, url, year in all_driver_links:
        if year not in driver_links_by_year:
            driver_links_by_year[year] = []
        driver_links_by_year[year].append((name, url))
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Process driver standings checkpoints
        print("Processing driver standings...")
        standings_results = []
        checkpoint_count = 0
        
        # Process each year
        for year, year_links in driver_links_by_year.items():
            # Create directory for the year
            year_dir = os.path.join(DATA_DIR, str(year))
            os.makedirs(year_dir, exist_ok=True)
            
            print(f"Processing {len(year_links)} drivers for year {year}")
            
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
            
            print(f"Processed {results_processed} drivers for year {year}")
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"Total execution time: {total_time:.2f} seconds")

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

if __name__ == "__main__":
    # First collect all driver links
    collect_links = asyncio.run(collect_driver_links())

    # Collect detailed profiles for current season drivers
    asyncio.run(collect_current_driver_profiles())

    # Then process all drivers with the collected links
    all_data = asyncio.run(scrape_f1_driver_data(collect_links[0]))

