from bs4 import BeautifulSoup
import aiohttp
import asyncio
import sys
import os
import json
import time

PROJECT_ROOT = os.getcwd()
sys.path.append(PROJECT_ROOT)
from src.utils.crawling_helpers import ssl_context, head, base_url, years

DATA_DIR = os.path.join(PROJECT_ROOT, "data", "f1_teams_data")
os.makedirs(DATA_DIR, exist_ok=True)
CHECKPOINTS_DIR = os.path.join(PROJECT_ROOT, "data", "f1_checkpoints")
os.makedirs(CHECKPOINTS_DIR, exist_ok=True)

async def scrape_teams_standing(session, year):
    """Scrape team standings for a specific year"""
    url = f"{base_url}/en/results/{year}/team"
    
    # Initialize variables with default empty values
    data = []
    headers = []
    team_links = []
    
    async with session.get(url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {url}. Status: {response.status}")
            return data, headers, team_links

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
            team_links = []
            
            for row in rows:
                cols = row.find_all('td')
                row_data = []
                
                for i, col in enumerate(cols):
                    if i == 1:  # Team name column
                        # Extract team name
                        team_a = col.find('a')
                        if team_a:
                            team_name = team_a.text.strip()
                            row_data.append(team_name)
                        else:
                            row_data.append(col.text.strip())
                    else:
                        row_data.append(col.text.strip())
                
                # Add year to each row
                row_data.append(str(year))
                data.append(row_data)
                
                # Extract team link
                team_link = cols[1].find('a')['href'] if cols[1].find('a') else None
                if team_link:
                    # Make sure the team_link starts with a slash if needed
                    if not team_link.startswith('/'):
                        team_link = f"/{team_link}"
                        
                    # Add the full URL
                    full_link = f"{base_url}/en/results/{year}{team_link}"
                    team_links.append((row_data[1], full_link, year))  # Add year to link tuple
                
        return data, headers, team_links

async def scrape_team_results(session, team_url):
    """Scrape detailed information for a specific team"""
    async with session.get(team_url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {team_url}. Status: {response.status}")
            return None, None, None
        
        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
        
        # Extract team code from URL
        url_parts = team_url.split('/')
        team_code = url_parts[-1] if len(url_parts) > 2 else None
        
        # Get the race results table
        table = soup.find('table', class_='f1-table-with-data')
        if not table:
            print(f"No results table found for {team_url}")
            return [], [], team_code
            
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
                
        return data, headers, team_code
    
async def process_team_data(session, team_link_tuple):
    """Process a team link to get detailed information"""
    team_name, url = team_link_tuple

    try:
        data, headers, team_code = await scrape_team_results(session, url)
        
        # Create a team details dictionary with all the data
        team_details = {
            'name': team_name,
            'team_code': team_code,
            'url': url,
            'headers': headers,
            'race_results': data
        }
        
        return team_details
    except Exception as e:
        print(f"Error processing team {team_name}: {e}")
        return None

async def collect_team_links():
    """Collect all team links across years"""
    all_team_links = []
    headers_teams = []
    teams = []
    
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    async with aiohttp.ClientSession(connector=connector) as session:          
        tasks = [scrape_teams_standing(session, year) for year in years]
        results = await asyncio.gather(*tasks)
    
        for team_data, header_team, team_links in results:
            teams.extend(team_data)
            all_team_links.extend([(link[0], link[1], link[2]) for link in team_links])

            if len(headers_teams) == 0:
                headers_teams = header_team
                
        # Save the teams data to a JSON file
        teams_data = {
            "headers": headers_teams,
            "teams": teams
        }    
        with open(os.path.join(DATA_DIR, "team_standing.json"), 'w', encoding='utf-8') as f:
            json.dump(teams_data, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(teams)} team standings to team_standing.json")
                
        return all_team_links, headers_teams, teams

async def scrape_team_profile(session, team_name, team_code):
    """Scrape detailed profile information for a team from the main teams page"""
    # Extract the team name part for the URL
    name_part = team_name.lower().replace(' ', '-')
    
    # Construct profile URL
    profile_url = f"{base_url}/en/teams/{name_part}.html"
    
    try:
        async with session.get(profile_url, headers=head) as response:
            if response.status != 200:
                print(f"Team profile not found: {profile_url}. Status: {response.status}")
                return None, None
            
            html = await response.text()
            soup = BeautifulSoup(html, 'lxml')
            
            # Get the team info section (with Full Team Name, Base, etc.)
            team_info_section = soup.find('div', class_='f1-dl')
            
            # Initialize lists for headers and data
            headers = ["name", "team_code", "profile_url"]
            data = [team_name, team_code, profile_url]
            
            if team_info_section:
                # Extract info from dt/dd pairs
                dt_elements = team_info_section.find_all('dt')
                dd_elements = team_info_section.find_all('dd')
                
                for dt, dd in zip(dt_elements, dd_elements):
                    # Convert header to lowercase with underscores
                    header = dt.text.strip().lower().replace(' ', '_')
                    data.append(dd.text.strip())
                    headers.append(header)

            # Get driver information
            drivers_section = soup.select('figure.bg-brand-white')
            
            if drivers_section and len(drivers_section) >= 1:
                # First driver
                driver1_div = drivers_section[0].find('figcaption').find('div')
                if driver1_div:
                    # Get driver number
                    driver1_number_elem = driver1_div.find('p', class_='f1-heading')
                    # Get driver name
                    driver1_name_elem = driver1_div.find_all('p', class_='f1-heading')[1] if len(driver1_div.find_all('p', class_='f1-heading')) > 1 else None
                    
                    if driver1_number_elem:
                        headers.append("driver_1_no")
                        data.append(driver1_number_elem.text.strip())
                    if driver1_name_elem:
                        headers.append("driver_1")
                        data.append(driver1_name_elem.text.strip())
            
            if drivers_section and len(drivers_section) >= 2:
                # Second driver
                driver2_div = drivers_section[1].find('figcaption').find('div')
                if driver2_div:
                    # Get driver number
                    driver2_number_elem = driver2_div.find('p', class_='f1-heading')
                    # Get driver name
                    driver2_name_elem = driver2_div.find_all('p', class_='f1-heading')[1] if len(driver2_div.find_all('p', class_='f1-heading')) > 1 else None
                    
                    if driver2_number_elem:
                        headers.append("driver_2_no")
                        data.append(driver2_number_elem.text.strip())
                    if driver2_name_elem:
                        headers.append("driver_2")
                        data.append(driver2_name_elem.text.strip())
            
            return headers, data
    except Exception as e:
        print(f"Error scraping profile for {team_name}: {e}")
        return None, None

async def scrape_teams_listing(session):
    """Scrape teams directly from the main F1 teams listing page"""
    url = f"{base_url}/en/teams"
    
    async with session.get(url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {url}. Status: {response.status}")
            return []

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
        
        # Find all team links
        team_items = soup.select('a[href^="/en/teams/"]')
        team_links = []
        
        for item in team_items:
            team_url = item['href']
            if team_url.count('/') >= 3:  # Make sure it's a team details page
                # Extract team name
                team_name_elem = item.select_one('.f1-heading[class*="font-bold"]')
                if team_name_elem:
                    team_name = team_name_elem.text.strip()
                    
                    # Extract position
                    position_elem = item.select_one('.f1-heading-black')
                    position = position_elem.text.strip() if position_elem else ""
                    
                    # Extract points
                    points_elem = item.select_one('.f1-heading-wide')
                    points = points_elem.text.strip() if points_elem else ""
                    
                    # Extract team logo URL
                    logo_elem = item.select_one('img[alt="' + team_name + '"]')
                    logo_url = logo_elem['src'] if logo_elem else ""
                    
                    # Extract team color - find the parent div that has text-COLOR class
                    team_card = item.select_one('div[class*="text-"]')
                    team_color = "#"
                    if team_card:
                        color_classes = [c for c in team_card.get('class', []) if c.startswith('text-') and not c == 'text-brand-black']
                        if color_classes:
                            team_color += color_classes[0].replace('text-', '')
                    
                    # Extract car image URL
                    car_img = item.select_one('.flex.items-baseline img')
                    car_img_url = car_img['src'] if car_img else ""
                    
                    # Get team code from URL
                    team_code = team_url.split('/')[-1]
                    
                    team_data = {
                        'name': team_name,
                        'team_code': team_code,
                        'position': position,
                        'points': points,
                        'logo_url': logo_url,
                        'car_img_url': car_img_url, 
                        'team_color': team_color,
                        'year': years[-1]  # Current year
                    }
                    
                    team_links.append(team_data)
        
        return team_links

async def collect_current_teams_data():
    """Collect comprehensive team data from the main teams page and individual profiles"""
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    timeout = aiohttp.ClientTimeout(total=60)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Get teams from main listing page
        teams_basic_data = await scrape_teams_listing(session)
        
        # Process each team to get detailed profile
        all_team_data = []
        
        for team in teams_basic_data:
            team_name = team['name']
            team_code = team['team_code']
            
            # Get detailed profile data
            headers, data = await scrape_team_profile(session, team_name, team_code)
            
            if headers and data:
                # Create a profile dictionary
                profile_dict = dict(zip(headers, data))
                
                # Merge with basic data
                for key, value in profile_dict.items():
                    if key not in team or not team[key]:  # Don't overwrite existing values
                        team[key] = value
                
                all_team_data.append(team)
                # print(f"Processed team: {team_name}")
            else:
                # Still add the basic team data even if profile fetch failed
                all_team_data.append(team)
                print(f"Added basic data for team: {team_name} (profile fetch failed)")
        
        # Save the complete team data
        current_year = years[-1]
        profiles_file = os.path.join(DATA_DIR, f"{current_year}_team_profiles.json")
        
        with open(profiles_file, 'w', encoding='utf-8') as f:
            json.dump(all_team_data, f, indent=2, ensure_ascii=False)
        
        print(f"Saved complete data for {len(all_team_data)} teams to {profiles_file}")
        
        return all_team_data

async def scrape_f1_team_data(all_team_links):
    """Scrape all F1 team data organized by year"""
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    # Create a longer timeout
    timeout = aiohttp.ClientTimeout(total=60)
    
    start_time = time.time()
    
    # Group team links by year
    team_links_by_year = {}
    for name, url, year in all_team_links:
        if year not in team_links_by_year:
            team_links_by_year[year] = []
        team_links_by_year[year].append((name, url))
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Process team standings checkpoints
        print("Processing team standings...")
        standings_results = []
        checkpoint_count = 0
        
        # Process each year
        for year, year_links in team_links_by_year.items():
            # Create directory for the year
            year_dir = os.path.join(DATA_DIR, str(year))
            os.makedirs(year_dir, exist_ok=True)
            
            # print(f"Processing {len(year_links)} teams for year {year}")
            
            # Process team results
            team_results = []
            results_processed = 0
            
            for i, link in enumerate(year_links):
                team_name, url = link
                
                # Process the team data
                result = await process_team_data(session, link)
                
                if result:
                    team_results.append(result)
                    
                    team_name = result['name'].lower()
                    # Sanitize filename by replacing invalid characters
                    team_name = team_name.replace('/', '_').replace('\\', '_')  # Handle path separators first
                    team_name = team_name.replace(' ', '_').replace('?', '').replace('*', '')
                    team_name = team_name.replace(':', '').replace('"', '').replace('<', '').replace('>', '')
                    team_file = os.path.join(year_dir, f"{team_name}.json")
                    
                    with open(team_file, 'w', encoding='utf-8') as f:
                        json.dump(result, f, indent=2, ensure_ascii=False)
                        
                    results_processed += 1
                
                # Save checkpoint every 100 teams or at the end
                checkpoint_file = os.path.join(CHECKPOINTS_DIR, "team_results_latest.json")
                if (i + 1) % 100 == 0 or i == len(year_links) - 1:
                    checkpoint_count += 1
                    with open(checkpoint_file, 'w', encoding='utf-8') as f:
                        json.dump(team_results, f, indent=2, ensure_ascii=False)
                
            # print(f"Processed {results_processed} teams for year {year}")
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"Total execution time: {total_time:.2f} seconds")
    
    # Delete checkpoint file after successful completion
    checkpoint_file = os.path.join(CHECKPOINTS_DIR, "team_results_latest.json")
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
        print(f"Deleted checkpoint file: {checkpoint_file}")

    # # Create a summary file
    # summary = {
    #     "total_standing_entries": sum(len(links) for links in team_links_by_year.values()),
    #     "total_team_files": sum(item["processed_count"] for item in standings_results),
    #     "years_processed": len(team_links_by_year),
    #     "execution_time": total_time
    # }
    
    # with open(os.path.join(DATA_DIR, "summary.json"), 'w') as f:
    #     json.dump(summary, f, indent=2)

    # Return the results
    return {
        "team_standings": standings_results,
        "execution_time": total_time
    }

async def scrape_team_async():
    # First collect all team links
    collect_links =  await collect_team_links()
    
    # Collect current teams data from the main teams page and detailed profiles
    current_teams = await collect_current_teams_data()

    # Then process all teams with the collected links
    all_data = await scrape_f1_team_data(collect_links[0])
    
    return True
    
def main():
    """Main function to run the team scraping"""
    asyncio.run(scrape_team_async())
    
if __name__ == "__main__":
    main()

