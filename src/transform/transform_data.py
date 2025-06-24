import os
import json
from collections import defaultdict
import re
import datetime
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.append(os.path.join(os.getcwd(), 'src'))

from utils.tranform_helpers import safe_float, safe_int, get_fact_table_name, generate_team_id, \
                                generate_unique_driver_id, normalize_driver_name, find_driver_id
from utils.country_list import country_list

from transform.transform_qualifying import extract_starting_grid_positions, is_multi_part_qualifying, process_combined_qualifying, \
                                 enforce_qualifying_schema, DATA_DIR, RACE_DATA_DIR

def discover_sessions():
    """Discover all session types and their schemas"""
    session_types = defaultdict(set)  # Maps session_name -> set of columns
    session_files = []  # List of (year, grand_prix, file_path, session_name)
    race_metadata = []  # List of (year, grand_prix, file_path)
    
    # Walk through the directory structure
    for year_dir in os.listdir(RACE_DATA_DIR):
        year_path = os.path.join(RACE_DATA_DIR, year_dir)
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
    drivers_data_dir = os.path.join(DATA_DIR, "f1_drivers_data")
    
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
                            'driver_id': driver_code,
                            'driver_name': normalize_driver_name(driver_name)
                            # Nationality removed
                        }
                except Exception as e:
                    print(f"Error processing driver file {file_path}: {e}")
    
    return drivers

def extract_driver_standings_facts(dimensions):
    """Extract driver standings from race_standing.json"""
    drivers_data_dir = os.path.join(DATA_DIR, "f1_drivers_data")
    driver_standings_file = os.path.join(drivers_data_dir, "race_standing.json")
    
    driver_standings = []
    
    try:
        with open(driver_standings_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for idx, row in enumerate(data.get('drivers', []), start=1):
            if len(row) >= 6:
                driver_name = row[1]
                car_name = row[3].strip() if row[3] else ""
                country_code = row[2]
                year = safe_int(row[5])
                
                # Simple era-based matching for Nelson Piquet
                driver_id = None

                if driver_name.lower() == "nelson piquet":
                    # Apply era-based matching using year
                    if year <= 1991:
                        # Update Sr. (NELPIQ01)
                        for d_id, d_info in dimensions['drivers'].items():
                            if d_info['driver_name'].lower() == driver_name.lower() and "01" in d_id:
                                driver_id = d_id
                                break
                    else:
                        # Update Jr. (NELPIQ02)
                        for d_id, d_info in dimensions['drivers'].items():
                            if d_info['driver_name'].lower() == driver_name.lower() and "02" in d_id:
                                driver_id = d_id
                                break
                elif driver_name.lower() == "robert doornbos":
                    # Apply era-based matching using year
                    if year == 2005:
                        for d_id, d_info in dimensions['drivers'].items():
                            if d_info['driver_name'].lower() == driver_name.lower() and "02" in d_id:
                                driver_id = d_id
                                break
                    else:
                        for d_id, d_info in dimensions['drivers'].items():
                            if d_info['driver_name'].lower() == driver_name.lower() and "01" in d_id:
                                driver_id = d_id
                                break
                            
                else:
                    # Regular name matching for other drivers
                    if not driver_id:
                        for d_id, d_info in dimensions['drivers'].items():
                            if d_info['driver_name'].lower() == driver_name.lower():
                                driver_id = d_id
                                break
                        
                # If no driver found, create new one
                if not driver_id:
                    driver_id = generate_unique_driver_id(
                        driver_name, 
                        [dimensions['drivers']]
                    )
                    
                    dimensions['drivers'][driver_id] = {
                        'driver_id': driver_id,
                    }
                
                # Handle team matching
                team_id = None
                if car_name:  # This check prevents creating empty teams
                    for t_id, t_info in dimensions['teams'].items():
                        if t_info['team_name'].lower() == car_name.lower():
                            team_id = t_id
                            break
                    
                    # Only create new team if car_name exists and no match found
                    if not team_id:
                        team_id = generate_team_id(car_name)
                        dimensions['teams'][team_id] = {
                            'team_id': team_id,
                            'team_name': car_name
                        }
                    
                driver_standings.append({
                    'driver_standing_id': idx,
                    'position': row[0], 
                    'driver_id': driver_id,
                    'country_code': country_code,
                    'team_id': team_id,
                    'points': safe_float(row[4]),  
                    'year': year
                })
                
    except Exception as e:
        print(f"Error processing driver standings: {e}")
    
    return driver_standings

def extract_team_standings_facts(dimensions):
    """Extract team standings from team_standing.json"""
    teams_data_dir = os.path.join(DATA_DIR, "f1_teams_data")
    team_standings_file = os.path.join(teams_data_dir, "team_standing.json")
    
    team_standings = []
    
    try:
        with open(team_standings_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for idx, row in enumerate(data.get('teams', []), start=1):
            if len(row) >= 4:
                team_name = row[1]
                
                team_id = None
                for t_id, t_info in dimensions['teams'].items():
                    if t_info['team_name'].lower() == team_name.lower():
                        team_id = t_id
                        break
                
                if not team_id:
                    team_id = generate_team_id(team_name)
                    dimensions['teams'][team_id] = {
                        'team_id': team_id,
                        'team_name': team_name
                    }
                
                team_standings.append({
                    'team_standing_id': idx,
                    'position': row[0],    
                    'team_id': team_id,
                    'points': safe_float(row[2]),  
                    'year': safe_int(row[3])
                })
    except Exception as e:
        print(f"Error processing team standings: {e}")
    
    return team_standings

def extract_teams_dimensions():
    """Extract teams from f1_teams_data folder"""
    teams = {}
    teams_data_dir = os.path.join(DATA_DIR, "f1_teams_data")
    
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
                    
                    if team_name:
                        team_id = generate_team_id(team_name) 
                        teams[team_id] = {
                            'team_id': team_id,
                            'team_name': team_name,
                            # 'url': team_data.get('url', '')
                        }
                except Exception as e:
                    print(f"Error processing team file {file_path}: {e}")
    
    return teams

def transform_race_results_to_facts(session_files, dimensions):
    """Transform session files into fact records"""
    # Map year+grand_prix to race_id
    race_id_map = {}
    for race_id, race_info in dimensions['races'].items():
        race_id_map[(race_info['year'], race_info['grand_prix'].lower().replace(' ', '_').replace('-', '_').replace("'", ""))] = race_id
    
    # Build lookup maps
    team_id_map = {t['team_name']: t['team_id'] for t in dimensions['teams'].values()}
    session_id_map = {s['session_name']: s['session_id'] for s in dimensions['sessions'].values()}
    
    # Track missing drivers to add to dimensions
    missing_drivers = {}
    
    # Create driver cache with year-aware keys for Nelson Piquet
    driver_cache = {}  # key = "driver_name|year" for Nelson Piquet, just "driver_name" for others
    
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
                
                if fact_table == 'pit_stops':
                    record.update({
                        'time' : None,
                        'total' : None,
                        'time_of_day' : None,
                    })
                    
                if fact_table == 'fastest_laps':
                    record.update({
                        'position': None,
                        'number': None,
                        'driver_id': None,
                        'team_id': None,
                        'lap': None,
                        'time': None,
                        'avg_speed': None,
                        'time_of_day': None,
                    })
                
                # Handle practice sessions with specific column mapping
                if fact_table == 'practice_results':
                    # Initialize all practice fields to null first
                    record['position'] = None
                    record['number'] = None
                    record['driver_id'] = None
                    record['team_id'] = None
                    record['time'] = None
                    record['laps'] = None
                    
                    # Then populate available data
                    for col, idx in header_indexes.items():
                        if idx < len(row) and row[idx]:
                            if col == 'DRIVER':
                                driver_name = row[idx]
                                driver_id =  find_driver_id(driver_name, year, driver_cache, dimensions, missing_drivers)
                                record['driver_id'] = driver_id
                            elif col == 'TEAM':
                                team_name = row[idx]
                                team_id = team_id_map.get(team_name)
                                if not team_id:
                                    # Generate team_id and add to dimensions
                                    team_id = generate_team_id(team_name)
                                    
                                    # Add to team dimensions
                                    dimensions['teams'][team_id] = {
                                        'team_id': team_id,
                                        'team_name': team_name
                                    }
                                    
                                    # Update the team_id_map for future lookups
                                    team_id_map[team_name] = team_id
                                    
                                record['team_id'] = team_id
                            elif col == 'POS':
                                record['position'] = row[idx]
                            elif col == 'NO':
                                record['number'] = safe_int(row[idx])
                            elif col == 'TIME / GAP':
                                record['time'] = row[idx]
                            elif col == 'LAPS':
                                record['laps'] = safe_int(row[idx])
                else:
                    # Handle other session types
                    for col, idx in header_indexes.items():
                        if idx < len(row) and row[idx]:
                            if col == 'DRIVER':
                                driver_name = row[idx]
                                driver_id = find_driver_id(driver_name, year, driver_cache, dimensions, missing_drivers)
                                record['driver_id'] = driver_id
                            elif col == 'TEAM':
                                team_name = row[idx]
                                team_id = team_id_map.get(team_name)
                                if not team_id:
                                    # Generate team_id and add to dimensions
                                    team_id = generate_team_id(team_name)
                                    
                                    # Add to team dimensions
                                    dimensions['teams'][team_id] = {
                                        'team_id': team_id,
                                        'team_name': team_name
                                    }
                                    
                                    # Update the team_id_map for future lookups
                                    team_id_map[team_name] = team_id
                                    
                                record['team_id'] = team_id
                                
                            elif col == 'TIME':
                                record['time'] = row[idx]
                            elif col == 'NO':
                                record['number'] = safe_int(row[idx])
                            elif col == 'LAPS':
                                # Only add 'laps' if not fastest_laps or pit_stop_summary table
                                if fact_table not in ['fastest_laps', 'pit_stops']:
                                    record['laps'] = safe_int(row[idx])
                            elif col == 'LAP':
                                record['lap'] = safe_int(row[idx])
                            elif col == 'PTS':
                                record['points'] = safe_float(row[idx])
                            elif col == "STOPS":
                                record['stops'] = safe_int(row[idx])
                            elif col in ['POS', 'RACE POS']:
                                record['position'] = row[idx]
                            elif col == 'TIME / RETIRED':
                                record['time'] = row[idx]
                            elif col == 'AVG SPEED':
                                record['avg_speed'] = safe_float(row[idx])
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

def extract_countries_dimensions(country_list):
    """Create countries dimension table from predefined list"""
    countries = {}
    
    for country_code, country_name in country_list:
        countries[country_code] = {
            'country_code': country_code,
            'country_name': country_name
        }
    
    logger.info(f"Created {len(countries)} countries")
    return countries

def main():
    logger.info("Starting F1 Data Transformation...")
    
    # Step 1: Discover all sessions
    logger.info("\n1. Discovering sessions...")
    session_types, session_files, race_metadata = discover_sessions()
    
    # print("Discovered Session Types:")
    # for session_name, headers_set in session_types.items():
    #     print(f"  {session_name}: {len(headers_set)} variations")
    
    # # Show all practice session variations
    # print("\nPractice Session Variations:")
    # for session_name, headers_set in session_types.items():
    #     print(f"  {session_name}:")
    #     for i, headers in enumerate(headers_set, 1):
    #         print(f"    Variation {i}: {list(headers)}")    
            
    logger.info(f"\nTotal session files: {len(session_files)}")
    logger.info(f"Total race metadata files: {len(race_metadata)}")
    
    # Step 2: Extract all dimensions
    logger.info("\n2. Extracting dimensions...")
    
    # Extract from f1_data folder
    race_sessions_dims = extract_race_sessions_dimensions(session_files, race_metadata)
    
    # Extract from dedicated folders
    drivers_dims = extract_drivers_dimensions()
    teams_dims = extract_teams_dimensions()
    countries_dims = extract_countries_dimensions(country_list)
    
    # Combine all dimensions
    dimensions = {
        'races': race_sessions_dims['races'],
        'sessions': race_sessions_dims['sessions'],
        'drivers': drivers_dims,
        'teams': teams_dims,
        'countries': countries_dims
    }
    
    logger.info(f"Found {len(dimensions['drivers'])} drivers")
    logger.info(f"Found {len(dimensions['teams'])} teams")
    logger.info(f"Found {len(dimensions['races'])} races")
    logger.info(f"Found {len(dimensions['sessions'])} sessions")
    
    # Step 3: Transform to facts
    logger.info("\n3. Transforming to fact tables...")
    fact_tables = {}
    fact_tables['team_standings'] = extract_team_standings_facts(dimensions)
    fact_tables['driver_standings'] = extract_driver_standings_facts(dimensions)
    
    race_results = transform_race_results_to_facts(session_files, dimensions)
    fact_tables.update(race_results)
    
    logger.info("Fact tables created:")
    for table_name, records in fact_tables.items():
        logger.info(f"  {table_name}: {len(records)} records")
    
    # Step 4: Save the results
    logger.info("\n4. Saving transformed data...")
    save_transformed_data(dimensions, fact_tables)
    
    logger.info("\n✅ Transformation complete!")
    
    return True

def save_transformed_data(dimensions, facts):
    """Save transformed data as JSON files"""
    TRANSFORM_DIR = os.path.join(DATA_DIR, "transformed_data")
    
    # Create output directories
    os.makedirs(os.path.join(TRANSFORM_DIR, "dimensions"), exist_ok=True)
    os.makedirs(os.path.join(TRANSFORM_DIR, "facts"), exist_ok=True)
    
    # Save dimensions
    for dim_name, dim_data in dimensions.items():
        dim_path = os.path.join(TRANSFORM_DIR, "dimensions", f'{dim_name}.json')
        dim_list = list(dim_data.values())
        
        with open(dim_path, 'w', encoding='utf-8') as f:
            json.dump(dim_list, f, indent=2, ensure_ascii=False)
            logger.info(f"  Saved {len(dim_list)} {dim_name} to {dim_name}.json")
    
    # Save facts
    for fact_name, fact_data in facts.items():
        fact_path = os.path.join(TRANSFORM_DIR, "facts", f'{fact_name}.json')
        with open(fact_path, 'w', encoding='utf-8') as f:
            json.dump(fact_data, f, indent=2, ensure_ascii=False)
            logger.info(f"  Saved {len(fact_data)} records to {fact_name}.json")
            
if __name__ == "__main__":
    main()