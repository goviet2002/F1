import os
import json
from collections import defaultdict
import re
import datetime
from tracemalloc import start

# Base data directory
DATA_DIR = r"C:\Users\anhvi\OneDrive\Desktop\F1 Projekt\data\f1_race_data"

import itertools

def normalize_name(name):
    parts = name.split()
    # Generate all permutations for names with 2 or 3 parts
    if 2 <= len(parts) <= 3:
        return [' '.join(p) for p in itertools.permutations(parts)]
    return [name]

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
                            'url': driver_data.get('url', '')
                        }
                except Exception as e:
                    print(f"Error processing driver file {file_path}: {e}")
    
    return drivers

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
                    
                    team_code = team_data.get('team_code')
                    team_name = team_data.get('name')
                    
                    if team_code and team_name:
                        teams[team_code] = {
                            'team_id': team_code,  # Use team_code as ID
                            'team_name': team_name,
                            'url': team_data.get('url', '')
                        }
                except Exception as e:
                    print(f"Error processing team file {file_path}: {e}")
    
    return teams

def get_fact_table_name(session_name, headers):
    """Automatically determine fact table name from session name"""
    session_lower = session_name.lower()
    
    # Handle qualifying with era detection first
    if 'qualifying' in session_lower:
        return 'qualifying_results'
    
    # Handle practice sessions (any word with "practice")
    if 'practice' in session_lower:
        return 'practice_results'
    
    # Handle race-related sessions
    if any(keyword in session_lower for keyword in ['race result', 'sprint', 'grid']):
        # But exclude qualifying sessions that might contain these words
        if 'qualifying' not in session_lower:
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
    
    # Extract starting grid positions
    starting_grid_map = extract_starting_grid_positions(race_id_map)
    
    # Build lookup maps
    driver_id_map = {}
    for d in dimensions['drivers'].values():
        for variant in normalize_name(d['driver_name']):
            driver_id_map[variant] = d['driver_id']
    team_id_map = {t['team_name']: t['team_id'] for t in dimensions['teams'].values()}
    session_id_map = {s['session_name']: s['session_id'] for s in dimensions['sessions'].values()}
    
    # Create fact tables
    fact_tables = defaultdict(list)
    fact_counters = defaultdict(int)
    
    # Group qualifying sessions by race for combining
    qualifying_sessions = defaultdict(list)  # race_key -> list of (session_name, file_path)
    other_sessions = []
    
    # Debug: Track Monaco 2019 sessions
    monaco_2019_sessions = []
    
    # First pass: separate qualifying sessions from others
    for year, grand_prix, file_path, session_name in session_files:
        race_key = (int(year), grand_prix.lower().replace(' ', '_').replace('-', '_').replace("'", ""))
        
        # Debug: collect Monaco 2019 sessions
        if year == 2019 and 'monaco' in grand_prix.lower():
            monaco_2019_sessions.append((session_name, file_path, is_multi_part_qualifying(session_name)))
        
        if is_multi_part_qualifying(session_name):
            qualifying_sessions[race_key].append((session_name, file_path))
        else:
            other_sessions.append((year, grand_prix, file_path, session_name))
    
    # Debug output for Monaco 2019
    if monaco_2019_sessions:
        print(f"\nDEBUG: Monaco 2019 sessions found:")
        for session_name, file_path, is_qualifying in monaco_2019_sessions:
            print(f"  '{session_name}' -> is_multi_part_qualifying: {is_qualifying}")
            print(f"    File: {file_path}")
        
        race_key = (2019, 'monaco')
        print(f"\nDEBUG: Monaco 2019 qualifying sessions grouped: {len(qualifying_sessions.get(race_key, []))}")
        for session_name, file_path in qualifying_sessions.get(race_key, []):
            print(f"  {session_name}: {file_path}")
    
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
            
            fact_table = get_fact_table_name(session_name, headers)
            
            for row in data.get('data', []):
                fact_counters[fact_table] += 1
                record = {
                    f'{fact_table[:-1]}_id': fact_counters[fact_table],
                    'race_id': race_id,
                    'session_id': session_id
                }
                for col, idx in header_indexes.items():
                    if idx < len(row) and row[idx]:
                        if col == 'Driver':
                            record['driver_id'] = driver_id_map.get(row[idx])
                        elif col == 'Car':
                            # Try to get team_id from map, else fallback to dash-joined name
                            team_name = row[idx]
                            team_id = team_id_map.get(team_name)
                            if not team_id:
                                team_id = team_name.replace(' ', '-')
                            record['team_id'] = team_id
                        elif col == 'Time':
                            record['q3'] = row[idx]
                        else:
                            col_name = col.lower().replace(' ', '_')
                            record[col_name] = row[idx]
                fact_tables[fact_table].append(record)
                
        except Exception as e:
            print(f"Error transforming {file_path}: {e}")
    
    # Process combined qualifying sessions
    process_combined_qualifying(qualifying_sessions, dimensions, fact_tables, fact_counters, starting_grid_map)
    
    # Enforce schema for qualifying_results
    enforce_qualifying_schema(fact_tables)

    return fact_tables

def is_multi_part_qualifying(session_name):
    """Check if this is part of multi-part qualifying or a single qualifying session"""
    session_lower = session_name.lower()
    
    # Check for multi-part qualifying (Q1, Q2, Q3, Overall Qualifying)
    is_multi_part = ('qualifying' in session_lower and 
                    (any(num in session_lower for num in ['1', '2', '3']) or 'overall' in session_lower) and
                    'sprint' not in session_lower)
    
    # Check for single qualifying sessions (just "Qualifying")
    is_single_qualifying = (session_lower == 'qualifying' or 
                           (session_lower.startswith('qualifying') and 
                            not any(num in session_lower for num in ['1', '2', '3']) and
                            'overall' not in session_lower and
                            'sprint' not in session_lower))
    
    return is_multi_part or is_single_qualifying

def process_combined_qualifying(qualifying_sessions, dimensions, fact_tables, fact_counters, starting_grid_map):
    """Combine multiple qualifying files into single records, using overall_qualifying if present."""
    race_id_map = {}
    for race_id, race_info in dimensions['races'].items():
        race_key = (race_info['year'], race_info['grand_prix'].lower().replace(' ', '_').replace('-', '_').replace("'", ""))
        race_id_map[race_key] = race_id

    # Get the generic "Qualifying" session ID
    qualifying_session_id = None
    for s in dimensions['sessions'].values():
        if s['session_name'].lower() == 'qualifying':
            qualifying_session_id = s['session_id']
            break

    for race_key, session_files in qualifying_sessions.items():
        year, grand_prix = race_key
        race_id = race_id_map.get(race_key)
        
        # Debug output for Monaco 2019
        if year == 2019 and grand_prix == 'monaco':
            print(f"\nDEBUG: Processing Monaco 2019 qualifying")
            print(f"  Race key: {race_key}")
            print(f"  Race ID: {race_id}")
            print(f"  Session files: {session_files}")
        
        if not race_id:
            print(f"Warning: No race_id found for {race_key}")
            continue

        # Load all qualifying data for this race
        qualifying_data = {}
        for session_name, file_path in session_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    qualifying_data[session_name] = data
                    
                    # Debug for Monaco 2019
                    if year == 2019 and grand_prix == 'monaco':
                        print(f"    Loaded session: {session_name}")
                        print(f"      File: {file_path}")
                        headers = data.get('header', [])
                        print(f"      Headers: {headers}")
                        print(f"      Rows: {len(data.get('data', []))}")
                        
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
                continue

        if not qualifying_data:
            continue

        # Combine the qualifying data
        combined_records = combine_qualifying_data(
            qualifying_data, race_id, dimensions, qualifying_session_id, starting_grid_map
        )
        
        # Debug for Monaco 2019
        if year == 2019 and grand_prix == 'monaco':
            print(f"    Combined records count: {len(combined_records)}")
            for record in combined_records[:3]:  # Show first 3 records
                print(f"      Sample record: {record}")

        # Add records to fact table
        for record in combined_records:
            fact_counters['qualifying_results'] += 1
            record['qualifying_result_id'] = fact_counters['qualifying_results']
            fact_tables['qualifying_results'].append(record)
def combine_qualifying_data(qualifying_data, race_id, dimensions, qualifying_session_id=None, starting_grid_map=None):
    """Combine multiple qualifying sessions into unified records"""
    # Get all drivers across all sessions
    all_drivers = set()
    for session_name, data in qualifying_data.items():
        headers = data.get('header', [])
        driver_idx = headers.index('Driver') if 'Driver' in headers else -1
        
        if driver_idx >= 0:
            for row in data.get('data', []):
                if driver_idx < len(row) and row[driver_idx]:
                    all_drivers.add(row[driver_idx])
    
    # Debug output for Monaco 2019
    if race_id == 1014:
        print(f"\nDEBUG: Monaco 2019 combine_qualifying_data")
        print(f"  Qualifying drivers found:")
        for driver in sorted(all_drivers):
            print(f"    '{driver}'")
        
        print(f"  Starting grid entries for race_id {race_id}:")
        vettel_found = False
        for (rid, driver), pos in starting_grid_map.items():
            if rid == race_id:
                print(f"    ({rid}, '{driver}') -> {pos}")
                if 'vettel' in driver.lower():
                    vettel_found = True
        
        if not vettel_found:
            print("    ❌ No Vettel found in starting grid!")
    
    # Create combined records
    combined_records = []
    driver_id_map = {d['driver_name']: d['driver_id'] for d in dimensions['drivers'].values()}
    team_id_map = {t['team_name']: t['team_id'] for t in dimensions['teams'].values()}
    
    # Helper function for sorting - give priority to actual Q sessions
    def sort_key(item):
        session_name = item[0]
        q_col = get_q_column_from_session(session_name)
        if q_col == 'q1':
            return 1
        elif q_col == 'q2':
            return 2
        elif q_col == 'q3':
            return 3
        else:
            return 0  # Sort "overall_qualifying" or single "qualifying" first
    
    for driver_name in all_drivers:
        # Get starting grid position
        starting_grid = starting_grid_map.get((race_id, driver_name)) if starting_grid_map else None
        
        # Debug output for Monaco 2019 Sebastian Vettel
        if race_id == 1014 and 'vettel' in driver_name.lower():
            print(f"\n  🔍 VETTEL DEBUG:")
            print(f"    Driver name in qualifying: '{driver_name}'")
            print(f"    Looking up key: ({race_id}, '{driver_name}')")
            print(f"    Starting grid result: {starting_grid}")
        
        # Get driver and team IDs
        driver_id = driver_id_map.get(driver_name)
        if not driver_id:
            if race_id == 1014:  # Only show for Monaco 2019 to reduce noise
                print(f"    Warning: Driver '{driver_name}' not found in driver dimensions")
            continue
        
        record = {
            'race_id': race_id,
            'session_id': qualifying_session_id,
            'driver_id': driver_id,
            'q1': None,
            'q2': None, 
            'q3': None,
            'pos': None,
            'quali_time': None,
            'starting_grid': starting_grid,
            'team_id': None,
            'no': None,
            'laps': None
        }
        
        for session_name, data in sorted(qualifying_data.items(), key=sort_key):
            headers = data.get('header', [])
            header_indexes = {col: idx for idx, col in enumerate(headers)}
            
            # Find this driver's row
            driver_idx = header_indexes.get('Driver', -1)
            if driver_idx < 0:
                continue
                
            for row in data.get('data', []):
                if (driver_idx < len(row) and 
                    row[driver_idx] == driver_name):
                    
                    # Debug for Monaco 2019 Vettel
                    if race_id == 1014 and 'vettel' in driver_name.lower():
                        print(f"    Found in session '{session_name}': {row}")
                        print(f"    Headers: {headers}")
                    
                    # Extract data from all available columns
                    for col_name, col_idx in header_indexes.items():
                        if col_idx < len(row) and row[col_idx]:
                            value = row[col_idx]
                            
                            if col_name == 'Pos' and record['pos'] is None:
                                try:
                                    record['pos'] = int(value)
                                except (ValueError, TypeError):
                                    record['pos'] = value
                            
                            elif col_name == 'No' and record['no'] is None:
                                try:
                                    record['no'] = int(value)
                                except (ValueError, TypeError):
                                    record['no'] = None
                            
                            elif col_name == 'Car' and record['team_id'] is None:
                                record['team_id'] = team_id_map.get(value, value.replace(' ', '-'))
                            
                            elif col_name == 'Laps' and record['laps'] is None:
                                record['laps'] = value
                            
                            # Handle Q1, Q2, Q3 columns directly
                            elif col_name == 'Q1' and record['q1'] is None:
                                record['q1'] = value
                            elif col_name == 'Q2' and record['q2'] is None:
                                record['q2'] = value
                            elif col_name == 'Q3' and record['q3'] is None:
                                record['q3'] = value
                            
                            # Handle generic Time column (for older formats)
                            elif col_name == 'Time':
                                # Determine which Q session this is based on session name
                                q_column = get_q_column_from_session(session_name)
                                if q_column and record[q_column] is None:
                                    record[q_column] = value
                                elif session_name.lower() == 'qualifying':
                                    # Single qualifying session
                                    record['quali_time'] = value
                    
                    # Debug for Monaco 2019 Vettel after extraction
                    if race_id == 1014 and 'vettel' in driver_name.lower():
                        print(f"    Extracted data: pos={record['pos']}, q1={record['q1']}, q2={record['q2']}, q3={record['q3']}")
                    
                    break
        
        # Set quali_time to the best available time
        if record['q3'] is not None:
            record['quali_time'] = record['q3']
        elif record['q2'] is not None:
            record['quali_time'] = record['q2']
        elif record['q1'] is not None:
            record['quali_time'] = record['q1']
        
        # Final debug for Monaco 2019 Vettel
        if race_id == 1014 and 'vettel' in driver_name.lower():
            print(f"    Final record: {record}")
        
        combined_records.append(record)
    
    return combined_records

def enforce_qualifying_schema(fact_tables):
    QUALIFYING_HEADER = [
        "qualifying_result_id", "race_id", "session_id", "pos", "no", "driver_id",
        "team_id", "q1", "q2", "q3", "quali_time", "laps", "starting_grid"
    ]
    if "qualifying_results" in fact_tables:
        new_records = []
        for rec_id, rec in enumerate(fact_tables["qualifying_results"], start=1):
            new_rec = {}
            for col in QUALIFYING_HEADER:
                if col == "qualifying_result_id":
                    new_rec[col] = rec.get(col, rec_id)
                elif col == "no":
                    # Convert 'no' to int, handle non-numeric values
                    no_value = rec.get(col)
                    if no_value is not None:
                        try:
                            new_rec[col] = int(no_value)
                        except (ValueError, TypeError):
                            new_rec[col] = None
                    else:
                        new_rec[col] = None
                else:
                    new_rec[col] = rec.get(col, None)
            
            # Ensure quali_time is set to the best time
            if new_rec['q3'] is not None:
                new_rec['quali_time'] = new_rec['q3']
            elif new_rec['q2'] is not None:
                new_rec['quali_time'] = new_rec['q2']
            elif new_rec['q1'] is not None:
                new_rec['quali_time'] = new_rec['q1']
                
            new_records.append(new_rec)
        fact_tables["qualifying_results"] = new_records
        
def get_q_column_from_session(session_name):
    """Map session name to Q column"""
    session_lower = session_name.lower()
    if '1' in session_lower:
        return 'q1'
    elif '2' in session_lower:
        return 'q2' 
    elif '3' in session_lower:
        return 'q3'
    else:
        return None  # Don't map overall_qualifying to any q-column

def extract_starting_grid_positions(race_id_map):
    """Extract starting grid positions for each race"""
    starting_grid_map = {}  # (race_id, driver_name) -> grid_position
    
    print("Extracting starting grid positions...")
    
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
                
            # Look for starting_grid.json
            grid_file = os.path.join(gp_path, 'starting_grid.json')
            if os.path.exists(grid_file):
                race_key = (year, gp_dir.lower().replace(' ', '_').replace('-', '_').replace("'", ""))
                race_id = race_id_map.get(race_key)
                
                if race_id:
                    try:
                        with open(grid_file, 'r', encoding='utf-8') as f:
                            grid_data = json.load(f)
                        
                        headers = grid_data.get('header', [])
                        driver_idx = headers.index('Driver') if 'Driver' in headers else -1
                        pos_idx = headers.index('Pos') if 'Pos' in headers else -1
                        
                        if driver_idx >= 0 and pos_idx >= 0:
                            for row in grid_data.get('data', []):
                                if len(row) > max(driver_idx, pos_idx):
                                    driver_name = row[driver_idx]
                                    grid_pos = row[pos_idx]
                                    
                                    # Convert position to integer
                                    try:
                                        grid_pos = int(grid_pos)
                                    except (ValueError, TypeError):
                                        grid_pos = None
                                    
                                    starting_grid_map[(race_id, driver_name)] = grid_pos
                                                            
                    except Exception as e:
                        print(f"Error processing starting grid {grid_file}: {e}")
                else:
                    print(f"  No race_id found for {race_key}")
    
    return starting_grid_map

def main():
    print("Starting F1 Data Transformation...")
    
    # Step 1: Discover all sessions
    print("\n1. Discovering sessions...")
    session_types, session_files, race_metadata = discover_sessions()
    
    print("Discovered Session Types:")
    for session_name, headers_set in session_types.items():
        print(f"  {session_name}: {len(headers_set)} variations")
    
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