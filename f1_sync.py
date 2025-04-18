import requests
from bs4 import BeautifulSoup
from datetime import datetime
import certifi
import pandas as pd
import time
import json
import os
import ssl

head = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
base_url = "https://www.formula1.com"

# Get years that statistics have been published
current_year = datetime.now().year
years = [year for year in range(1950, current_year + 1)]

def scrape_races_year_sync(year):
    # URL of the page
    url = f"{base_url}/en/results/{year}/races"
    
    # Send a GET request to the URL
    response = requests.get(url, headers=head)
    if response.status_code != 200:
        print(f"Failed to load {url}. Status: {response.status_code}")
        return [], [], []
    
    # Parse the HTML content using BeautifulSoup
    html = response.text
    soup = BeautifulSoup(html, 'lxml')
    
    # Find table
    table = soup.find('table', class_='f1-table-with-data')
    data = []
    headers = []
    race_links = []
    
    if table:
        headers = [header.text.strip() for header in table.find('thead').find_all('th')]
        
        rows = table.find('tbody').find_all('tr')
        
        for row in rows:
            cols = row.find_all('td')
            row_data = []
            
            for i, col in enumerate(cols):
                if i == 2: #Driver column
                    winner = col.text.strip().replace("\xa0", " ")[:-3]
                    row_data.append(winner)
                else:
                    row_data.append(col.text.strip())
                    
            # Append the row data to the data list
            data.append(row_data)
            
            # Extract race link
            race_link = cols[0].find('a')['href']
            full_link = f"{base_url}/en/results/{year}/{race_link}"
            race_links.append((row_data[0], full_link))
            
    return data, headers, race_links

def scrape_race_location_sync(race_url):
    response = requests.get(race_url, headers=head)
    if response.status_code != 200:
        print(f"Failed to load {race_url}. Status: {response.status_code}")
        return None, None, None
    
    html = response.text
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
    return None, None, None

def scrape_race_sessions_sync(race_url):
    response = requests.get(race_url, headers=head)
    if response.status_code != 200:
        print(f"Failed to load {race_url}. Status: {response.status_code}")
        return []
    
    html = response.text
    soup = BeautifulSoup(html, 'lxml')
    session_items = soup.find_all("ul", class_="f1-sidebar-wrapper")
    
    if not session_items:
        return []
        
    li = session_items[0].find_all("li")
    sessions = []
    
    for item in li:
        link = item.find("a")
        if link:
            session_name = link.text.strip()
            session_link = f"{base_url}{link['href']}"
            sessions.append((session_name, session_link))
            
    return sessions

def scrape_race_results_sync(session_url, session_name=None):
    response = requests.get(session_url, headers=head)
    if response.status_code != 200:
        print(f"Failed to load {session_url}. Status: {response.status_code}")
        return None
    
    html = response.text
    soup = BeautifulSoup(html, 'lxml')
        
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

def process_race_location_sync(race_link_tuple):
    grand_prix, url = race_link_tuple
    year = url.split('/results/')[1].split('/')[0]

    try:
        race_date, circuit, city = scrape_race_location_sync(url)
        if race_date:
            return [grand_prix, circuit, city, year, race_date]
    except Exception as e:
        print(f"Error processing {url}: {e}")
    return None

def collect_race_links_sync():
    all_race_links = []
    headers_race = []
    races = []
    
    for year in years:
        race, header_race, race_links = scrape_races_year_sync(year)
        races.extend(race)
        all_race_links.extend([(link[0], link[1]) for link in race_links])
        
        if len(headers_race) == 0 and header_race:
            headers_race = header_race
    
    return all_race_links, headers_race, races

def scrape_f1_data_sync():
    start_time = time.time()
    
    all_race_links, headers_race, races = collect_race_links_sync()
    print(f"Found {len(all_race_links)} race links")
    
    # Process Race Location sequentially
    print("Processing race locations...")
    location_results = []
    headers_race_location = ['Grand Prix', 'Circuit', 'Country/City', 'Year', 'Date']
    
    for i, link in enumerate(all_race_links):
        result = process_race_location_sync(link)
        location_results.append(result)
    
    race_location = [result for result in location_results if result is not None]
    print(f"Processed {len(race_location)} race locations")
    
    # Process Race Sessions sequentially
    print("Getting race sessions...")
    session_results = []
    for i, link in enumerate(all_race_links):
        sessions = scrape_race_sessions_sync(link[1])
        if sessions:
            session_results.append(sessions)
    
    all_sessions = []
    for result in session_results:
        all_sessions.extend(result)
    
    print(f"Found {len(all_sessions)} session results to process")
    
    # Scrape Race Results sequentially
    print("Processing race results...")
    race_result = {}
    for i, task in enumerate(all_sessions):
        result = scrape_race_results_sync(task[1], task[0])
        if result is not None:
            headers, data, url, session_name = result
            race_result[url] = {
                "header": headers,
                "data": data,
                "session_name": session_name
            }
    
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Processed {len(race_result)} race results")
    print(f"Total synchronous execution time: {total_time:.2f} seconds")
    
    # Return the results
    all_data = {
        "race_location": race_location,
        "race_sessions": all_sessions,
        "race_result": race_result,
        "execution_time": total_time
    }
    
    return all_data

if __name__ == "__main__":
    print("Starting synchronous F1 data collection...")
    all_data = scrape_f1_data_sync()
  
    # Save complete data
    print("Saving complete data...")
    with open('f1_data_sync.json', 'w') as f:
        json.dump(all_data, f, indent=2)
    
    print(f"Complete! Execution time: {all_data['execution_time']:.2f} seconds")