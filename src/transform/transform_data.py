import os
import json
from collections import defaultdict
import re
import datetime
import sys
import urllib.parse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.helper import safe_float, safe_int

from transform_qualifying import extract_starting_grid_positions, is_multi_part_qualifying, process_combined_qualifying, \
                                 enforce_qualifying_schema, normalize_name, DATA_DIR

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

def extract_race_sessions_dimensions(session_files, race_metadata_files):
    """Extract dimension data from all files"""
    races = {}    # race_id -> race_info
    sessions = {} # session_name -> session_id
    
    EXCLUDED_SESSIONS = {
        'Overall Qualifying',
        'Qualifying 1', 
        'Qualifying 2',
        'Sprint Grid',
        'Starting Grid'
    }
    
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
                    
                    # Parse the end date first (which always has complete information)
                    end_parts = end_part.split()
                    
                    # Fix the start part based on what's missing
                    start_parts = start_part.split()
                    if len(start_parts) == 2:  # Only day and month
                        # Check if months are different
                        if len(end_parts) >= 2 and start_parts[1] != end_parts[1]:
                            # Different months, just add the year
                            start_part = f"{start_part} {end_parts[-1]}"
                        else:
                            # Same month or can't determine, add month and year from end
                            start_part = f"{start_parts[0]} {' '.join(end_parts[1:])}"
                    elif len(start_parts) == 1:  # Only day
                        # Add month and year from end
                        start_part = f"{start_part} {' '.join(end_parts[1:])}"
                    
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
    
    def get_session_category(session_name):
        """Determine session category and sequence number based on name"""
        session_name_lower = session_name.lower()
        
        # Category definitions
        categories = {
            'practice': 1,
            'qualifying': 2,
            'sprint': 3,
            'race': 4
        }
        
        # Determine category using the dictionary
        category_value = 5  # Default for unknown
        
        if 'practice' in session_name_lower:
            category_value = categories['practice']
        elif any(word in session_name_lower for word in ['qualifying']) and 'sprint' not in session_name_lower:
            category_value = categories['qualifying']
        elif any(word in session_name_lower for word in ['sprint']):
            category_value = categories['sprint']
        elif any(word in session_name_lower for word in ['warm up', 'grid', 'race', 'fastest', 'pit stop']):
            category_value = categories['race']
        
        # Determine sequence within category
        sequence = 0
        
        if category_value == 3:  # Sprint category
            if 'qualifying' in session_name_lower or 'shootout' in session_name_lower:
                sequence = 1  # Sprint qualifying first
            elif 'grid' in session_name_lower:
                sequence = 2  # Sprint grid second
            elif 'sprint' in session_name_lower and 'qualifying' not in session_name_lower:
                sequence = 3  # Sprint race third
        
        elif category_value == 4:  # Race category
            if 'warm up' in session_name_lower:
                sequence = 1  # Warm up first
            elif 'grid' in session_name_lower:
                sequence = 2  # Starting grid second
            elif 'race' in session_name_lower and 'result' in session_name_lower:
                sequence = 3  # Race result third
            elif 'fastest' in session_name_lower:
                sequence = 4  # Fastest laps fourth
            elif 'pit stop' in session_name_lower:
                sequence = 5  # Pit stops fifth
        
        else:
            # Extract number if present (e.g., "Practice 1" -> 1)
            number_match = re.search(r'(\d+)', session_name)
            if number_match:
                sequence = int(number_match.group(1))
        
        # Calculate final order value: category * 100 + sequence
        return int(category_value * 100 + sequence)    

    # Build sessions dimension with dynamic ordering
    session_names = sorted(set([s for _, _, _, s in session_files]))
    session_names = [name for name in session_names if name not in EXCLUDED_SESSIONS]
    
    for session_name in sorted(session_names, key=get_session_category):
        session_id = len(sessions) + 1
        session_order = get_session_category(session_name)
        sessions[session_name] = {
            'session_id': session_id,
            'session_name': session_name,
            'session_order': session_order,
            'category': session_order // 100  # Extract just the category portion
        }
    return {
        'races': races,
        'sessions': sessions
    }
    
def extract_drivers_dimensions():
    """Extract drivers from f1_drivers_data folder"""
    drivers = {}
    drivers_data_dir = r"C:\Users\anhvi\OneDrive\Desktop\F1 Projekt\data\f1_drivers_data"
    
    for year_dir in os.listdir(drivers_data_dir):
        year_path = os.path.join(drivers_data_dir, year_dir)
        if not os.path.isdir(year_path):
            continue
            
        for file_name in os.listdir(year_path):
            if file_name.endswith('.json'):
                file_path = os.path.join(year_path, file_name)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        driver_data = json.load(f)
                    
                    driver_code = driver_data.get('driver_code')
                    driver_name = driver_data.get('name')
                    
                    if driver_code and driver_name:
                        drivers[driver_code] = {
                            'driver_id': driver_code,  # Use driver_code as ID
                            'driver_name': driver_name,
                            # 'url': driver_data.get('url', '')
                        }
                except Exception as e:
                    print(f"Error processing driver file {file_path}: {e}")
    
    return drivers

def generate_team_id(team_name):
    # Step 1: Convert to lowercase and replace spaces with hyphens
    team_name = team_name.lower().replace(" ", "-").replace("/", "-")

    # Step 2: Split the name into parts (manufacturer, team, sponsor, etc.)
    team_name_parts = team_name.split("-")

    # Step 3: Abbreviate each part (keep it simple by taking the first letter or first 3 characters)
    # For example, "Ferrari" -> "FER", "McLaren" -> "McL"
    abbreviated_parts = [part[:3].upper() for part in team_name_parts]

    # Step 4: Combine the abbreviated parts to form the team ID
    team_id = "-".join(abbreviated_parts)

    return team_id

def extract_teams_dimensions():
    """Extract teams from f1_teams_data folder"""
    teams = {}
    teams_data_dir = r"C:\Users\anhvi\OneDrive\Desktop\F1 Projekt\data\f1_teams_data"
    
    for year_dir in os.listdir(teams_data_dir):
        year_path = os.path.join(teams_data_dir, year_dir)
        if not os.path.isdir(year_path):
            continue
            
        for file_name in os.listdir(year_path):
            if file_name.endswith('.json'):
                file_path = os.path.join(year_path, file_name)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        team_data = json.load(f)
                    
                    team_name = team_data.get('name')
                    team_code = generate_team_id(team_name)
                    
                    if team_code and team_name:
                        teams[team_code] = {
                            'team_id': generate_team_id(team_code),
                            'team_name': team_name,
                            # 'url': team_data.get('url', '')
                        }
                except Exception as e:
                    print(f"Error processing team file {file_path}: {e}")
    
    return teams

def get_fact_table_name(session_name):
    """Automatically determine fact table name from session name"""
    session_lower = session_name.lower()
    
    if "Grid".lower() in session_lower:
        return None
    
    # Handle qualifying with era detection first
    if 'qualifying' in session_lower or 'shootout' in session_lower:
        return 'qualifying_results'
    
    # Handle practice sessions (any word with "practice")
    if 'practice' in session_lower or "Warm up".lower() in session_lower:
        return 'practice_results'
    
    # Handle race-related sessions
    if any(keyword in session_lower for keyword in ['race result', 'sprint']):
        if 'qualifying' not in session_lower and 'shootout' not in session_lower:
            return 'race_results'
    
    # Handle other specific session types
    if 'fastest' in session_lower:
        return 'fastest_laps'
    
    if 'pit stop' in session_lower:
        return 'pit_stops'
    
    # Default for unknown session types
    return f'{session_name}'

def transform_to_facts(session_files, dimensions):
    """Transform session files into fact records"""
    # Map year+grand_prix to race_id
    race_id_map = {}
    for race_id, race_info in dimensions['races'].items():
        race_id_map[(race_info['year'], race_info['grand_prix'].lower().replace(' ', '_').replace('-', '_').replace("'", ""))] = race_id
    
    # Build lookup maps
    driver_id_map = {}
    for d in dimensions['drivers'].values():
        for variant in normalize_name(d['driver_name']):
            driver_id_map[variant] = d['driver_id']
    
    team_id_map = {t['team_name']: t['team_id'] for t in dimensions['teams'].values()}
    session_id_map = {s['session_name']: s['session_id'] for s in dimensions['sessions'].values()}
    
    # Track missing drivers to add to dimensions
    missing_drivers = {}
    
    # Create fact tables
    fact_tables = defaultdict(list)
    fact_counters = defaultdict(int)
    
    # Group qualifying sessions by race for combining
    qualifying_sessions = defaultdict(list)  # race_key -> list of (session_name, file_path)
    other_sessions = []
    
    # First pass: separate qualifying sessions from others
    for year, grand_prix, file_path, session_name in session_files:
        race_key = (int(year), grand_prix.lower().replace(' ', '_').replace('-', '_').replace("'", ""))
        
        if session_name == "Starting Grid":
            continue
        
        if is_multi_part_qualifying(session_name):
            qualifying_sessions[race_key].append((session_name, file_path))
        else:
            other_sessions.append((year, grand_prix, file_path, session_name))
    
    # Process regular sessions normally
    for year, grand_prix, file_path, session_name in other_sessions:
        race_key = (int(year), grand_prix.lower().replace(' ', '_').replace('-', '_').replace("'", ""))
        race_id = race_id_map.get(race_key)
        
        if race_id is None:
            print(f"Warning: No race_id found for {race_key}")
            continue
    
        session_id = session_id_map.get(session_name)        
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            headers = data.get('header', [])
            header_indexes = {col: idx for idx, col in enumerate(headers)}
            
            fact_table = get_fact_table_name(session_name)
            if fact_table is None:
                continue
            
            for row in data.get('data', []):
                fact_counters[fact_table] += 1
                record = {
                    f'{fact_table[:-1]}_id': fact_counters[fact_table],
                    'race_id': race_id,
                    'session_id': session_id
                }
                
                # Handle practice sessions with specific column mapping
                if fact_table == 'practice_results':
                    # Initialize all practice fields to null first
                    record['pos'] = None
                    record['no'] = None
                    record['driver_id'] = None
                    record['team_id'] = None
                    record['time'] = None
                    record['gap'] = None
                    record['laps'] = None
                    
                    # Then populate available data
                    for col, idx in header_indexes.items():
                        if idx < len(row) and row[idx]:
                            if col == 'Driver':
                                driver_name = row[idx]
                                driver_id = driver_id_map.get(driver_name)
                                
                                # If driver not found, create a new ID
                                if not driver_id:
                                    # Generate ID from name (first 3 chars of first name + first 3 chars of last name + 01)
                                    name_parts = driver_name.split()
                                    if len(name_parts) >= 2:
                                        first_name = name_parts[0][:3].upper()
                                        last_name = name_parts[-1][:3].upper()
                                        driver_id = f"{first_name}{last_name}01"
                                    else:
                                        # Single name fallback
                                        driver_id = f"{driver_name[:6].upper().replace(' ', '')}01"
                                    
                                    # Add to missing drivers dict and driver_id_map
                                    missing_drivers[driver_id] = {
                                        'driver_id': driver_id,
                                        'driver_name': ' '.join(driver_name.split()),
                                        # 'url': None
                                    }
                                    driver_id_map[driver_name] = driver_id
                                    
                                    # print(f"Created new driver ID: {driver_name} -> {driver_id}")
                                
                                record['driver_id'] = driver_id
                            elif col == 'Car':
                                team_name = row[idx]
                                team_id = team_id_map.get(team_name)
                                if not team_id:
                                    team_id = team_name.replace(' ', '-')
                                record['team_id'] = team_id
                            elif col == 'Pos':
                                record['pos'] = row[idx]
                            elif col == 'No':
                                record['no'] = safe_int(row[idx])
                            elif col == 'Time':
                                record['time'] = row[idx]
                            elif col == 'Gap':
                                record['gap'] = row[idx]
                            elif col == 'Laps':
                                record['laps'] = safe_int(row[idx])
                else:
                    # Handle other session types with original logic
                    for col, idx in header_indexes.items():
                        if idx < len(row) and row[idx]:
                            if col == 'Driver':
                                driver_name = row[idx]
                                driver_id = driver_id_map.get(driver_name)
                                
                                # If driver not found, create a new ID
                                if not driver_id:
                                    name_parts = driver_name.split()
                                    if len(name_parts) >= 2:
                                        first_name = name_parts[0][:3].upper()
                                        last_name = name_parts[-1][:3].upper()
                                        driver_id = f"{first_name}{last_name}01"
                                    else:
                                        driver_id = f"{driver_name[:6].upper().replace(' ', '')}01"
                                    
                                    missing_drivers[driver_id] = {
                                        'driver_id': driver_id,
                                        'driver_name': ' '.join(driver_name.split()),
                                        # 'url': None
                                    }
                                    driver_id_map[driver_name] = driver_id
                                    
                                    # print(f"Created new driver ID: {driver_name} -> {driver_id}")
                                
                                record['driver_id'] = driver_id
                            elif col == 'Car':
                                team_name = row[idx]
                                team_id = team_id_map.get(team_name)
                                if not team_id:
                                    team_id = team_name.replace(' ', '-')
                                record['team_id'] = team_id
                            elif col == 'Time':
                                record['time'] = row[idx]
                            elif col == 'No':
                                record['no'] = safe_int(row[idx])
                            elif col == 'Laps':
                                record['laps'] = safe_int(row[idx])
                            elif col == 'Lap':
                                record['lap'] = safe_int(row[idx])
                            elif col == 'Pts':
                                record['pts'] = safe_float(row[idx])
                            elif col == "Stops":
                                record['stops'] = safe_int(row[idx])
                            else:
                                col_name = col.lower().replace(' ', '_')
                                record[col_name] = row[idx]
                
                fact_tables[fact_table].append(record)
                
        except Exception as e:
            print(f"Error transforming {file_path}: {e}")
    
    # Add missing drivers to dimensions
    dimensions['drivers'].update(missing_drivers)
    
    # Extract starting grid positions and times
    starting_grid_map, starting_grid_times, sprint_grid_map, sprint_grid_times = extract_starting_grid_positions(race_id_map)
    
    # Process combined qualifying sessions
    process_combined_qualifying(qualifying_sessions, dimensions, fact_tables, fact_counters, 
                              starting_grid_map, starting_grid_times, sprint_grid_map, sprint_grid_times)
    
    # Enforce schema for qualifying_results
    enforce_qualifying_schema(fact_tables)

    return fact_tables

def main():
    print("Starting F1 Data Transformation...")
    
    # Step 1: Discover all sessions
    print("\n1. Discovering sessions...")
    session_types, session_files, race_metadata = discover_sessions()
    
    print("Discovered Session Types:")
    for session_name, headers_set in session_types.items():
        print(f"  {session_name}: {len(headers_set)} variations")
    
    # # Show all practice session variations
    # print("\nPractice Session Variations:")
    # for session_name, headers_set in session_types.items():
    #     print(f"  {session_name}:")
    #     for i, headers in enumerate(headers_set, 1):
    #         print(f"    Variation {i}: {list(headers)}")    
            
    print(f"\nTotal session files: {len(session_files)}")
    print(f"Total race metadata files: {len(race_metadata)}")
    
    # Step 2: Extract all dimensions
    print("\n2. Extracting dimensions...")
    
    # Extract from f1_data folder
    race_sessions_dims = extract_race_sessions_dimensions(session_files, race_metadata)
    
    # Extract from dedicated folders
    drivers_dims = extract_drivers_dimensions()
    teams_dims = extract_teams_dimensions()
    
    # Combine all dimensions
    dimensions = {
        'races': race_sessions_dims['races'],
        'sessions': race_sessions_dims['sessions'],
        'drivers': drivers_dims,
        'teams': teams_dims
    }
    
    print(f"Found {len(dimensions['drivers'])} drivers")
    print(f"Found {len(dimensions['teams'])} teams")
    print(f"Found {len(dimensions['races'])} races")
    print(f"Found {len(dimensions['sessions'])} sessions")
    
    # Step 3: Transform to facts
    print("\n3. Transforming to fact tables...")
    fact_tables = transform_to_facts(session_files, dimensions)
    
    print("Fact tables created:")
    for table_name, records in fact_tables.items():
        print(f"  {table_name}: {len(records)} records")
    
    # Step 4: Save the results
    print("\n4. Saving transformed data...")
    save_transformed_data(dimensions, fact_tables)
    
    print("\n✅ Transformation complete!")

def save_transformed_data(dimensions, facts):
    """Save transformed data as JSON files"""
    TRANSFORM_DIR = r"C:\Users\anhvi\OneDrive\Desktop\F1 Projekt\data\transformed"
    
    # Create output directories
    os.makedirs(os.path.join(TRANSFORM_DIR, "dimensions"), exist_ok=True)
    os.makedirs(os.path.join(TRANSFORM_DIR, "facts"), exist_ok=True)
    
    # Save dimensions
    for dim_name, dim_data in dimensions.items():
        dim_path = os.path.join(TRANSFORM_DIR, "dimensions", f'{dim_name}.json')
        dim_list = list(dim_data.values())
        
        with open(dim_path, 'w', encoding='utf-8') as f:
            json.dump(dim_list, f, indent=2, ensure_ascii=False)
            print(f"  Saved {len(dim_list)} {dim_name} to {dim_name}.json")
    
    # Save facts
    for fact_name, fact_data in facts.items():
        fact_path = os.path.join(TRANSFORM_DIR, "facts", f'{fact_name}.json')
        with open(fact_path, 'w', encoding='utf-8') as f:
            json.dump(fact_data, f, indent=2, ensure_ascii=False)
            print(f"  Saved {len(fact_data)} records to {fact_name}.json")
            
if __name__ == "__main__":
    main()