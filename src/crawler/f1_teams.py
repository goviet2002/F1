from bs4 import BeautifulSoup
import aiohttp
import asyncio
import sys
import os
import json
import time
import logging
import re

logger = logging.getLogger(__name__)

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

    data = []
    headers = []
    team_links = []

    async with session.get(url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {url}. Status: {response.status}")
            return data, headers, team_links

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')

        # Find the correct table
        table = soup.find('table', class_='f1-table-with-data')
        if not table:
            return data, headers, team_links

        # Extract headers
        headers = ['Pos', 'Team', 'Pts', 'Year']
        # headers = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]
        # if 'Year' not in headers:
        #     headers.append('Year')

        rows = table.find('tbody').find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 3:
                continue  # skip malformed rows

            # Position
            position = cols[0].get_text(strip=True)

            # Team name and link
            team_td = cols[1]
            team_a = team_td.find('a')
            if team_a:
                team_name = team_a.get_text(strip=True)
                team_link = team_a.get('href', '')
                # Normalize link
                if team_link.startswith('/../../'):
                    team_link = team_link.replace('/../../', '/')
                if not team_link.startswith('/'):
                    team_link = '/' + team_link
                full_link = f"{base_url}{team_link}"
            else:
                team_name = team_td.get_text(strip=True)
                full_link = None

            # Points
            points = cols[2].get_text(strip=True)

            # Compose row data
            row_data = [position, team_name, points, str(year)]
            data.append(row_data)

            # Save team link tuple if available
            if full_link:
                team_links.append((team_name, full_link, year))

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
            
        # Get headers automatically, but since format changed, we keep the old format
        # headers = [header.text.strip() for header in table.find('thead').find_all('th')]
        headers = ['Grand prix', 'Date', 'Pts']
        
        # Get race results
        rows = table.find('tbody').find_all('tr')
        data = []
        
        for row in rows:
            cols = row.find_all('td')
            row_data = []
            for i, col in enumerate(cols):
                if i == 0:
                    # Grand Prix cell: get only the visible text after the SVG
                    a = col.find('a')
                    if a:
                        # Get the last text node (should be the visible name)
                        grand_prix = ""
                        for content in reversed(a.contents):
                            if isinstance(content, str) and content.strip():
                                grand_prix = content.strip()
                                break
                        text = grand_prix
                    else:
                        text = col.get_text(strip=True)
                else:
                    text = col.get_text(strip=True)
                row_data.append(text)
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
        
        logger.info(f"Saved {len(teams)} team standings to team_standing.json")
                
        return all_team_links, headers_teams, teams

async def scrape_team_profile(session, team_name, team_code):
    """Scrape detailed profile information for a team from the main teams page"""
    profile_url = f"{base_url}/en/teams/{team_code}"

    try:
        async with session.get(profile_url, headers=head) as response:
            if response.status != 200:
                print(f"Team profile not found: {profile_url}. Status: {response.status}")
                return None, None

            html = await response.text()
            soup = BeautifulSoup(html, 'lxml')

            profile = {
                "name": team_name,
                "team_code": team_code,
                "profile_url": profile_url,
            }

            # --- Logo image ---
            logo_img = soup.find("img", class_="relative z-40 h-px-32")
            if logo_img:
                profile["logo_url"] = logo_img.get("src", "")
            else:
                profile["logo_url"] = ""

            # --- Car image ---
            car_img = soup.find("img", class_="relative z-40 max-w-full max-h-[90px] md:max-h-[127px] lg:max-h-[183px]")
            if car_img:
                profile["car_img_url"] = car_img.get("src", "")
            else:
                profile["car_img_url"] = ""

            # --- Remove car_img if present ---
            if "car_img" in profile:
                del profile["car_img"]

            # --- Drivers ---
            profile["drivers"] = []
            for card in soup.select('a[data-f1rd-a7s-click="driver_card_click"]'):
                driver = {}
                # Driver name
                name_elem = card.select_one('p.typography-module_display-l-bold__m1yaJ')
                if name_elem:
                    driver["name"] = name_elem.text.strip()
                # Driver image
                img_elem = card.select_one('div.absolute img')
                if img_elem:
                    driver["img"] = img_elem.get("src", "")
                # Nationality (flag title)
                flag_elem = card.select_one('svg[role="presentation"] title')
                if flag_elem:
                    nationality = flag_elem.text.strip()
                    if nationality.lower().startswith("flag of "):
                        nationality = nationality[8:].strip()
                    driver["nationality"] = nationality
                profile["drivers"].append(driver)

            # --- All <dl> blocks (team info, statistics, summary) ---
            for dl in soup.find_all('dl'):
                for div in dl.find_all('div', class_='DataGrid-module_item__cs9Zd'):
                    dt = div.find('dt')
                    dd = div.find('dd')
                    if dt and dd:
                        key = dt.text.strip().lower().replace(' ', '_')
                        value = dd.text.strip()
                        profile[key] = value

            return list(profile.keys()), list(profile.values())
    except Exception as e:
        print(f"Error scraping profile for {team_name}: {e}")
        return None, None

async def scrape_teams_listing(session):
    """Scrape teams directly from the main F1 teams listing page (2025 structure)"""
    url = f"{base_url}/en/teams"
    async with session.get(url, headers=head) as response:
        if response.status != 200:
            print(f"Failed to load {url}. Status: {response.status}")
            return []

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')

        teams = []
        # Each team card
        for card in soup.select('a.group\\/team-card'):
            team = {}
            # Team name
            name_elem = card.select_one('p.typography-module_display-l-bold__m1yaJ')
            team['name'] = name_elem.text.strip() if name_elem else ""
            # Team code (from href)
            href = card.get('href', '')
            team['team_code'] = href.split('/')[-1] if href else ""
            team['profile_url'] = base_url + href if href else ""
            # Team logo
            logo_elem = card.select_one('.TeamLogo-module_teamlogo__lA3j1 img')
            team['logo_url'] = logo_elem['src'] if logo_elem else ""
            # Car image
            car_img_elem = card.select_one('span.relative img.absolute')
            team['car_img_url'] = car_img_elem['src'] if car_img_elem else ""
            # Team color (from style)
            style = card.get('style', '')
            import re
            match = re.search(r'--f1-team-colour:\s*([^;]+);', style)
            team['team_color'] = match.group(1) if match else ""
            # Drivers
            team['drivers'] = []
            for driver in card.select('span.flex.gap-px-8.rounded-s.items-center'):
                driver_name = " ".join([
                    x.text.strip() for x in driver.select('span.typography-module_body-xs-regular__0B0St, span.typography-module_body-xs-bold__TovJz')
                ])
                driver_img_elem = driver.select_one('img')
                driver_img = driver_img_elem['src'] if driver_img_elem else ""
                team['drivers'].append({
                    'name': driver_name,
                    'img': driver_img
                })
            teams.append(team)
        return teams

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
                    if key not in team or not team[key]:
                        team[key] = value
                
                # Ensure 'name' is set to 'full_team_name' if 'name' is empty
                if not team.get("name") and team.get("full_team_name"):
                    team["name"] = team["full_team_name"]
                
                # Remove 'full_team_name' to avoid redundancy
                if "full_team_name" in team:
                    del team["full_team_name"]
                
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
        
        logger.info(f"Saved complete data for {len(all_team_data)} teams to {profiles_file}")
        
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
        logger.info("Processing team standings...")
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
    
    logger.info(f"\nCompleted teams data collection in {total_time:.2f} seconds")
    
    # Delete checkpoint file after successful completion
    checkpoint_file = os.path.join(CHECKPOINTS_DIR, "team_results_latest.json")
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
        logger.info(f"Deleted checkpoint file: {checkpoint_file}")

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

