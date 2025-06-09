import os
import json
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.path import get_project_root
from utils.helper import normalize_name

PROJECT_ROOT = get_project_root()
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "f1_race_data")
os.makedirs(DATA_DIR, exist_ok=True)

def is_multi_part_qualifying(session_name):
    """Check if this is part of multi-part qualifying or a single qualifying session"""
    session_lower = session_name.lower()
    
    # Include ALL qualifying sessions (regular qualifying, sprint qualifying, and sprint shootout)
    if not ('qualifying' in session_lower or 'shootout' in session_lower):
        return False
    
    # Check for multi-part qualifying (Q1, Q2, Q3, Overall Qualifying)
    is_multi_part = (any(num in session_lower for num in ['1', '2', '3']) or 'overall' in session_lower)
    
    # Check for single qualifying sessions
    is_single_qualifying = (session_lower == 'qualifying' or 
                           session_lower == 'sprint qualifying' or
                           session_lower == 'sprint shootout' or  # Add this line
                           (session_lower.startswith('qualifying') and 
                            not any(num in session_lower for num in ['1', '2', '3']) and
                            'overall' not in session_lower) or
                           (session_lower.startswith('sprint') and
                            ('qualifying' in session_lower or 'shootout' in session_lower) and
                            not any(num in session_lower for num in ['1', '2', '3']) and
                            'overall' not in session_lower))
    
    return is_multi_part or is_single_qualifying

def combine_qualifying_data(qualifying_data, race_id, dimensions, qualifying_session_id=None, 
                           starting_grid_map=None, starting_grid_times=None,
                           sprint_grid_map=None, sprint_grid_times=None):
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
    
    # Create combined records
    combined_records = []
    driver_id_map = {}
    for d in dimensions['drivers'].values():
        for variant in normalize_name(d['driver_name']):
            driver_id_map[variant] = d['driver_id']
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
    
    # Determine if this is a sprint qualifying session - FIX THIS LOGIC
    is_sprint_qualifying = any('sprint' in session_name.lower() for session_name in qualifying_data.keys())
    
    for driver_name in all_drivers:
        # Choose appropriate grid data based on session type - THIS IS WHERE THE FIX GOES
        if is_sprint_qualifying:
            starting_grid = sprint_grid_map.get((race_id, driver_name)) if sprint_grid_map else None
            starting_grid_quali_time = sprint_grid_times.get((race_id, driver_name)) if sprint_grid_times else None
        else:
            starting_grid = starting_grid_map.get((race_id, driver_name)) if starting_grid_map else None
            starting_grid_quali_time = starting_grid_times.get((race_id, driver_name)) if starting_grid_times else None
        
        # Get driver and team IDs
        driver_id = driver_id_map.get(driver_name)
        if not driver_id:
            continue
        
        record = {
            'race_id': race_id,
            'session_id': qualifying_session_id,
            'driver_id': driver_id,
            'q1': None,
            'q2': None, 
            'q3': None,
            'pos': None,
            'quali_time': starting_grid_quali_time,
            'starting_grid': starting_grid,
            'team_id': None,
            'no': None,
            'laps': None
        }
        
        # Extract individual Q times from qualifying sessions
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
                    
                    # Extract data from all available columns
                    for col_name, col_idx in header_indexes.items():
                        if col_idx < len(row) and row[col_idx]:
                            value = row[col_idx]
                            
                            if col_name == 'Pos' and record['pos'] is None:
                                try:
                                    record['pos'] = value
                                except (ValueError, TypeError):
                                    record['pos'] = None
                            
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
                                elif is_sprint_qualifying and starting_grid_quali_time:
                                    record['quali_time'] = starting_grid_quali_time
                                elif (session_name.lower() == 'qualifying' or 
                                    session_name.lower() == 'overall qualifying'):
                                    # Use session Time if we don't have starting grid time
                                    if record['quali_time'] is None:
                                        record['quali_time'] = value
                    
                    break
        
        # Fallback: If no starting grid time, use the best available Q time
        if record['quali_time'] is None:
            if record['q3'] is not None:
                record['quali_time'] = record['q3']
            elif record['q2'] is not None:
                record['quali_time'] = record['q2']
            elif record['q1'] is not None:
                record['quali_time'] = record['q1']
        
        combined_records.append(record)
    
    return combined_records

def process_combined_qualifying(qualifying_sessions, dimensions, fact_tables, fact_counters, 
                              starting_grid_map, starting_grid_times, sprint_grid_map, sprint_grid_times):
    """Process qualifying sessions - handle missing sprint qualifying files"""
    race_id_map = {}
    for race_id, race_info in dimensions['races'].items():
        race_key = (race_info['year'], race_info['grand_prix'].lower().replace(' ', '_').replace('-', '_').replace("'", ""))
        race_id_map[race_key] = race_id

    session_id_map = {s['session_name']: s['session_id'] for s in dimensions['sessions'].values()}

    for race_key, session_files in qualifying_sessions.items():
        year, grand_prix = race_key
        race_id = race_id_map.get(race_key)
        
        if not race_id:
            print(f"Warning: No race_id found for {race_key}")
            continue

        # Group sessions by type (sprint vs regular)
        sprint_sessions = {}
        regular_sessions = {}
        
        for session_name, file_path in session_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Separate sprint and regular qualifying
                if 'sprint' in session_name.lower():
                    sprint_sessions[session_name] = data
                else:
                    regular_sessions[session_name] = data
                        
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
                continue

        # Check if this race has sprint grid but no sprint qualifying
        race_folder = os.path.join(DATA_DIR, str(year), grand_prix)
        sprint_grid_file = os.path.join(race_folder, 'sprint_grid.json')
        has_sprint_grid = os.path.exists(sprint_grid_file)
        
        # If no sprint qualifying but has sprint grid, create qualifying from grid data
        if not sprint_sessions and has_sprint_grid:
            print(f"Race {race_id} ({year} {grand_prix}): No sprint qualifying file, using sprint_grid.json")
            try:
                with open(sprint_grid_file, 'r', encoding='utf-8') as f:
                    sprint_grid_data = json.load(f)
                
                # Convert sprint grid to qualifying format
                sprint_qualifying_data = convert_sprint_grid_to_qualifying(sprint_grid_data)
                sprint_sessions['Sprint Qualifying'] = sprint_qualifying_data
                
            except Exception as e:
                print(f"Error loading sprint grid {sprint_grid_file}: {e}")

        # Process sprint qualifying sessions (including converted ones)
        if sprint_sessions:            
            # Find the correct session ID
            sprint_session_names = list(sprint_sessions.keys())
            actual_sprint_session_name = sprint_session_names[0]
            qualifying_session_id = session_id_map.get(actual_sprint_session_name)
            if not qualifying_session_id:
                # Fallback to generic Sprint Qualifying
                qualifying_session_id = session_id_map.get('Sprint Qualifying')
            
            # Process sprint qualifying
            combined_records = combine_qualifying_data(
                sprint_sessions, race_id, dimensions, qualifying_session_id, 
                starting_grid_map, starting_grid_times, sprint_grid_map, sprint_grid_times
            )

            # Add sprint qualifying records to fact table
            for record in combined_records:
                fact_counters['qualifying_results'] += 1
                record['qualifying_result_id'] = fact_counters['qualifying_results']
                fact_tables['qualifying_results'].append(record)

        # Process regular qualifying sessions
        if regular_sessions:            
            qualifying_session_id = session_id_map.get('Qualifying')
                        
            # Process regular qualifying
            combined_records = combine_qualifying_data(
                regular_sessions, race_id, dimensions, qualifying_session_id, 
                starting_grid_map, starting_grid_times, sprint_grid_map, sprint_grid_times
            )

            # Add regular qualifying records to fact table
            for record in combined_records:
                fact_counters['qualifying_results'] += 1
                record['qualifying_result_id'] = fact_counters['qualifying_results']
                fact_tables['qualifying_results'].append(record)

def convert_sprint_grid_to_qualifying(sprint_grid_data):
    """Convert sprint_grid.json data to qualifying session format with same columns as regular qualifying"""
    headers = sprint_grid_data.get('header', [])
    data_rows = sprint_grid_data.get('data', [])
    
    # Create qualifying-style data structure with all standard qualifying columns
    qualifying_data = {
        'header': ['Pos', 'No', 'Driver', 'Car', 'Q1', 'Q2', 'Q3', 'Time', 'Laps'],
        'data': [],
        'session_name': 'Sprint Qualifying'
    }
    
    # Map existing columns from sprint_grid
    driver_idx = headers.index('Driver') if 'Driver' in headers else -1
    pos_idx = headers.index('Pos') if 'Pos' in headers else -1
    no_idx = headers.index('No') if 'No' in headers else -1
    car_idx = headers.index('Car') if 'Car' in headers else -1
    time_idx = headers.index('Time') if 'Time' in headers else -1
    
    for row in data_rows:
        if len(row) == 0 or not row[0]:  # Skip empty rows
            continue
            
        # Create row with all qualifying columns: [Pos, No, Driver, Car, Q1, Q2, Q3, Time, Laps]
        new_row = ['', '', '', '', '', '', '', '', '']
        
        # Copy available data from sprint_grid
        if pos_idx >= 0 and len(row) > pos_idx:
            new_row[0] = row[pos_idx]  # Pos
        if no_idx >= 0 and len(row) > no_idx:
            new_row[1] = row[no_idx]   # No
        if driver_idx >= 0 and len(row) > driver_idx:
            new_row[2] = row[driver_idx]  # Driver
        if car_idx >= 0 and len(row) > car_idx:
            new_row[3] = row[car_idx]   # Car
        
        # Q1, Q2, Q3 - leave empty (indices 4, 5, 6)
        new_row[4] = ''  # Q1 - null/empty
        new_row[5] = ''  # Q2 - null/empty  
        new_row[6] = ''  # Q3 - null/empty
        
        # Time from grid file
        if time_idx >= 0 and len(row) > time_idx:
            new_row[7] = row[time_idx]  # Time
        else:
            new_row[7] = ''  # Time - empty if not available
            
        # Laps - leave empty (index 8)
        new_row[8] = ''  # Laps - null/empty
        
        qualifying_data['data'].append(new_row)
    
    return qualifying_data

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
                elif col == "laps":
                    # Convert 'laps' to int, handle non-numeric values
                    laps_value = rec.get(col)
                    if laps_value is not None:
                        try:
                            new_rec[col] = int(laps_value)
                        except (ValueError, TypeError):
                            new_rec[col] = None
                    else:
                        new_rec[col] = None
                else:
                    new_rec[col] = rec.get(col, None)
            
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
    """Extract starting grid positions and qualifying times for each race"""
    starting_grid_map = {}  # (race_id, driver_name) -> grid_position
    starting_grid_times = {}  # (race_id, driver_name) -> qualifying_time
    sprint_grid_map = {}  # (race_id, driver_name) -> sprint_grid_position
    sprint_grid_times = {}  # (race_id, driver_name) -> sprint_qualifying_time
    
    print("Extracting starting grid positions and times...")
    
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
                
            race_key = (year, gp_dir.lower().replace(' ', '_').replace('-', '_').replace("'", ""))
            race_id = race_id_map.get(race_key)
            
            if not race_id:
                continue
                
            # Look for regular starting_grid.json
            grid_file = os.path.join(gp_path, 'starting_grid.json')
            if os.path.exists(grid_file):
                try:
                    with open(grid_file, 'r', encoding='utf-8') as f:
                        grid_data = json.load(f)
                    
                    headers = grid_data.get('header', [])
                    driver_idx = headers.index('Driver') if 'Driver' in headers else -1
                    pos_idx = headers.index('Pos') if 'Pos' in headers else -1
                    time_idx = headers.index('Time') if 'Time' in headers else -1

                    if driver_idx >= 0 and pos_idx >= 0:
                        for row in grid_data.get('data', []):
                            if len(row) > max(driver_idx, pos_idx):
                                driver_name = row[driver_idx]
                                grid_pos = row[pos_idx]
                                quali_time = row[time_idx] if time_idx >= 0 and len(row) > time_idx else None
                                
                                # Convert position to integer
                                try:
                                    grid_pos = int(grid_pos)
                                except (ValueError, TypeError):
                                    grid_pos = None
                                
                                starting_grid_map[(race_id, driver_name)] = grid_pos
                                if quali_time:
                                    starting_grid_times[(race_id, driver_name)] = quali_time
                                                        
                except Exception as e:
                    print(f"Error processing starting grid {grid_file}: {e}")
            
            # Look for sprint_grid.json
            sprint_grid_file = os.path.join(gp_path, 'sprint_grid.json')
            if os.path.exists(sprint_grid_file):
                try:
                    with open(sprint_grid_file, 'r', encoding='utf-8') as f:
                        sprint_data = json.load(f)
                    
                    headers = sprint_data.get('header', [])
                    driver_idx = headers.index('Driver') if 'Driver' in headers else -1
                    pos_idx = headers.index('Pos') if 'Pos' in headers else -1
                    time_idx = headers.index('Time') if 'Time' in headers else -1

                    if driver_idx >= 0 and pos_idx >= 0:
                        for row in sprint_data.get('data', []):
                            if len(row) > max(driver_idx, pos_idx):
                                driver_name = row[driver_idx]
                                grid_pos = row[pos_idx]
                                sprint_time = row[time_idx] if time_idx >= 0 and len(row) > time_idx else None
                                
                                # Convert position to integer
                                try:
                                    grid_pos = int(grid_pos)
                                except (ValueError, TypeError):
                                    grid_pos = None
                                
                                sprint_grid_map[(race_id, driver_name)] = grid_pos
                                if sprint_time:
                                    sprint_grid_times[(race_id, driver_name)] = sprint_time
                                                        
                except Exception as e:
                    print(f"Error processing sprint grid {sprint_grid_file}: {e}")
    
    return starting_grid_map, starting_grid_times, sprint_grid_map, sprint_grid_times
