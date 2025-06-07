import os
import json
from collections import defaultdict
import re
import datetime
from tracemalloc import start

# Base data directory
DATA_DIR = r"C:\Users\anhvi\OneDrive\Desktop\F1 Projekt\data\f1_data"

def discover_sessions():
    """Discover all session types and their schemas"""
    session_types = defaultdict(set)  # Maps session_name -> set of columns
    session_files = []  # List of (year, grand_prix, file_path, session_name)
    race_metadata = []  # List of (year, grand_prix, file_path)
    
    # Walk through the directory structure
    for year_dir in os.listdir(DATA_DIR):
        year_path = os.path.join(DATA_DIR, year_dir)
        if not os.path.isdir(year_path):
            continue
            
        try:
            year = int(year_dir)
        except ValueError:
            continue
            
        for gp_dir in os.listdir(year_path):
            gp_path = os.path.join(year_path, gp_dir)
            if not os.path.isdir(gp_path):
                continue
                
            grand_prix = gp_dir
            
            for file_name in os.listdir(gp_path):
                if not file_name.endswith('.json'):
                    continue
                    
                file_path = os.path.join(gp_path, file_name)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Handle race_metadata differently
                    if 'race_metadata' in file_name:
                        race_metadata.append((year, grand_prix, file_path))
                        # Still add to session_types for completeness
                        session_types['race_metadata'].add(())
                        continue
                    
                    # Extract session name and headers
                    session_name = data.get('session_name', 
                                           re.sub(r'\.json$', '', file_name))
                    
                    headers = tuple(data.get('header', []))
                    session_types[session_name].add(headers)
                    
                    session_files.append((year, grand_prix, file_path, session_name))
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
    
    return session_types, session_files, race_metadata

def extract_dimensions(session_files, race_metadata_files):
    """Extract dimension data from all files"""
    drivers = {}  # driver_name -> driver_id
    teams = {}    # team_name -> team_id
    races = {}    # race_id -> race_info
    sessions = {} # session_name -> session_id
    
    # First process race_metadata to build races dimension
    for year, grand_prix, file_path in race_metadata_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            race_id = len(races) + 1
            
            # Convert date string to standard format if possible
            date_str = metadata.get('date', '')
            try:
                # Try to handle date ranges like "25 - 27 Oct 2024"
                if ' - ' in date_str:
                    # Split the date range
                    date_parts = date_str.split(' - ')
                    start_part = date_parts[0]
                    end_part = date_parts[1]
                    
                    if len(start_part.split()) < 3:
                        # Month and year are in the end part
                        month_year = ' '.join(end_part.split()[1:])
                        start_part = f"{start_part} {month_year}"
                        
                    # Parse both dates
                    start_date_obj = datetime.datetime.strptime(start_part, '%d %b %Y')
                    end_date_obj = datetime.datetime.strptime(end_part, '%d %b %Y')
                    
                    # Format in standard format
                    start_date = start_date_obj.strftime('%d-%m-%Y')
                    end_date = end_date_obj.strftime('%d-%m-%Y')
                else:
                    # Single date - use the same for both start and end
                    date_obj = datetime.datetime.strptime(date_str, '%d %b %Y')
                    start_date = end_date = date_obj.strftime('%d-%m-%Y')
                    
            except Exception as e:
                # Keep original if parsing fails
                print(f"Date parsing error for '{date_str}': {e}")
                start_date = end_date = date_str
                
            races[race_id] = {
                'race_id': race_id,
                'year': int(year),
                'grand_prix': metadata.get('grand_prix', grand_prix),
                'circuit': metadata.get('circuit', ''),
                'city': metadata.get('city', ''),
                'start_date': start_date,
                'end_date': end_date
            }
        except Exception as e:
            print(f"Error processing metadata {file_path}: {e}")
            
            # Create minimal race entry if metadata processing fails
            race_id = len(races) + 1
            races[race_id] = {
                'race_id': race_id,
                'year': int(year),
                'grand_prix': grand_prix,
            }
    
    # Define session types and their order
    session_order = {
        'Practice': 1,
        'Practice 1': 2,
        'Practice 2': 3,
        'Practice 3': 4,
        'Practice 4': 5,
        'Qualifying': 10,
        'Qualifying 1': 11,
        'Qualifying 2': 12,
        'Overall Qualifying': 15,
        'Sprint Qualifying': 20,
        'Sprint Shootout': 21,
        'Sprint Grid': 22,
        'Sprint': 23,
        'Warm up': 30,
        'Starting Grid': 40,
        'Race Result': 50,
        'Fastest Laps': 60,
        'Pit Stop Summary': 70
    }
    
    # Build sessions dimension
    for session_name in sorted(set([s for _, _, _, s in session_files]), 
                              key=lambda x: session_order.get(x, 100)):
        sessions[session_name] = len(sessions) + 1
    
    # Process all session files to extract drivers and teams
    for year, grand_prix, file_path, session_name in session_files:
        # Extract drivers and teams from file
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            headers = data.get('header', [])
            driver_idx = headers.index('Driver') if 'Driver' in headers else -1
            car_idx = headers.index('Car') if 'Car' in headers else -1
            
            if driver_idx >= 0 or car_idx >= 0:
                for row in data.get('data', []):
                    if driver_idx >= 0 and driver_idx < len(row):
                        driver_name = row[driver_idx]
                        if driver_name and driver_name not in drivers:
                            drivers[driver_name] = len(drivers) + 1
                    
                    if car_idx >= 0 and car_idx < len(row):
                        team_name = row[car_idx]
                        if team_name and team_name not in teams:
                            teams[team_name] = len(teams) + 1
        except Exception as e:
            print(f"Error extracting dimensions from {file_path}: {e}")
    
    return {
        'drivers': {name: {'driver_id': did, 'driver_name': name} 
                   for name, did in drivers.items()},
        'teams': {name: {'team_id': tid, 'team_name': name} 
                 for name, tid in teams.items()},
        'races': races,
        'sessions': {name: {'session_id': sid, 'session_name': name, 
                          'session_order': session_order.get(name, 100)} 
                   for name, sid in sessions.items()}
    }

def main():
    session_types, session_files, race_metadata = discover_sessions()
    
    print("Discovered Session Types:")
    for session_name, headers_set in session_types.items():
        print(f"\n{session_name}:")
        for headers in headers_set:
            print(f"{headers}")

    print(f"\nTotal session files: {len(session_files)}")
    print(f"Total race metadata files: {len(race_metadata)}")

if __name__ == "__main__":
    main()