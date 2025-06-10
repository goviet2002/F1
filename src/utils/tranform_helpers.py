
import itertools

# Utility functions for data transformation and normalization
def normalize_name(name):
    parts = name.split()
    # Generate all permutations for names with 2 or 3 parts
    if 2 <= len(parts) <= 3:
        return [' '.join(p) for p in itertools.permutations(parts)]
    return [name]

def safe_int(value):
    """Safely convert value to int, return None if conversion fails"""
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_float(value):
    """Safely convert value to float, return None if conversion fails"""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

# Function to get the full nationality name from a code
def get_full_nationality(nationality_code):
    """Convert nationality abbreviation to full country name"""
    nationality_map = {
        'ABU': 'United Arab Emirates',
        'AFG': 'Afghanistan',
        'ALB': 'Albania',
        'ALG': 'Algeria',
        'AND': 'Andorra',
        'ANG': 'Angola',
        'ARG': 'Argentina',
        'ARM': 'Armenia',
        'AUS': 'Australia',
        'AUT': 'Austria',
        'AZE': 'Azerbaijan',
        'BAH': 'Bahrain',
        'BAN': 'Bangladesh',
        'BAR': 'Barbados',
        'BEL': 'Belgium',
        'BER': 'Bermuda',
        'BOL': 'Bolivia',
        'BRA': 'Brazil',
        'BRN': 'Brunei',
        'BUL': 'Bulgaria',
        'BUR': 'Burkina Faso',
        'CAM': 'Cambodia',
        'CAN': 'Canada',
        'CHI': 'Chile',
        'CHN': 'China',
        'COL': 'Colombia',
        'CRC': 'Costa Rica',
        'CRO': 'Croatia',
        'CUB': 'Cuba',
        'CYP': 'Cyprus',
        'CZE': 'Czech Republic',
        'DEN': 'Denmark',
        'ECU': 'Ecuador',
        'EGY': 'Egypt',
        'ENG': 'England',
        'ESP': 'Spain',
        'EST': 'Estonia',
        'ETH': 'Ethiopia',
        'FIN': 'Finland',
        'FRA': 'France',
        'GBR': 'Great Britain',
        'GER': 'Germany',
        'GHA': 'Ghana',
        'GRE': 'Greece',
        'GUA': 'Guatemala',
        'HKG': 'Hong Kong',
        'HUN': 'Hungary',
        'IND': 'India',
        'INA': 'Indonesia',
        'IRL': 'Ireland',
        'IRN': 'Iran',
        'IRQ': 'Iraq',
        'ISL': 'Iceland',
        'ISR': 'Israel',
        'ITA': 'Italy',
        'JAM': 'Jamaica',
        'JPN': 'Japan',
        'JOR': 'Jordan',
        'KAZ': 'Kazakhstan',
        'KEN': 'Kenya',
        'KOR': 'South Korea',
        'KUW': 'Kuwait',
        'LAT': 'Latvia',
        'LIB': 'Lebanon',
        'LIE': 'Liechtenstein',
        'LTU': 'Lithuania',
        'LUX': 'Luxembourg',
        'MAD': 'Madagascar',
        'MAL': 'Malaysia',
        'MAR': 'Morocco',
        'MEX': 'Mexico',
        'MON': 'Monaco',
        'MYA': 'Myanmar',
        'NED': 'Netherlands',
        'NEP': 'Nepal',
        'NOR': 'Norway',
        'NZL': 'New Zealand',
        'PAK': 'Pakistan',
        'PAN': 'Panama',
        'PAR': 'Paraguay',
        'PER': 'Peru',
        'PHI': 'Philippines',
        'POL': 'Poland',
        'POR': 'Portugal',
        'PUR': 'Puerto Rico',
        'QAT': 'Qatar',
        'RHO': 'Rhodesia',
        'ROU': 'Romania',
        'RUS': 'Russia',
        'SAU': 'Saudi Arabia',
        'SCO': 'Scotland',
        'SEN': 'Senegal',
        'SIN': 'Singapore',
        'SLO': 'Slovenia',
        'SVK': 'Slovakia',
        'SWE': 'Sweden',
        'SWI': 'Switzerland',
        'SYR': 'Syria',
        'THA': 'Thailand',
        'TUN': 'Tunisia',
        'TUR': 'Turkey',
        'UAE': 'United Arab Emirates',
        'UKR': 'Ukraine',
        'URU': 'Uruguay',
        'USA': 'United States',
        'UZB': 'Uzbekistan',
        'VEN': 'Venezuela',
        'VIE': 'Vietnam',
        'WAL': 'Wales',
        'YUG': 'Yugoslavia',
        'ZAM': 'Zambia',
        'ZIM': 'Zimbabwe',
        'RSA': 'South Africa',
        'TCH': 'Czechoslovakia',
        'DDR': 'East Germany',
        'FRG': 'West Germany',
        'URS': 'Soviet Union'
    }
    
    return nationality_map.get(nationality_code.upper(), nationality_code)

# Function to get the fact table name based on session name
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

# Add Nationality to driver dimensions
def update_driver_dimensions_with_nationality(dimensions, driver_standings):
    """Update driver dimensions with nationality from standings data"""
    # Create a mapping of driver_id to nationality info
    id_to_nationality = {}
    for standing in driver_standings:
        driver_id = standing['driver_id']
        nationality_code = standing['country_code']
        nationality_full = get_full_nationality(nationality_code)
        
        if driver_id and driver_id not in id_to_nationality:
            id_to_nationality[driver_id] = {
                'country_code': nationality_code,
                'country': nationality_full
            }
    
    # Update driver dimensions with both code and full name
    for driver_id, driver_info in dimensions['drivers'].items():
        if driver_id in id_to_nationality:
            nat_info = id_to_nationality[driver_id]
            driver_info['country_code'] = nat_info['country_code']
            driver_info['country'] = nat_info['country']

# Function to generate a unique team ID based on the team name
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

def generate_unique_driver_id(driver_name, existing_ids_sources):
    """
    Generate a unique driver ID, incrementing the number if ID already exists
    
    Args:
        driver_name (str): The driver's full name
        existing_ids_sources (list): List of dictionaries/mappings to check for existing IDs
                                   e.g. [dimensions['drivers'], missing_drivers, driver_id_map.values()]
    
    Returns:
        str: Unique driver ID in format like "MARDON01", "MARDON02", etc.
    """
    # Generate base ID from name (first 3 chars of first name + first 3 chars of last name)
    name_parts = driver_name.split()
    if len(name_parts) >= 2:
        first_name = name_parts[0][:3].upper()
        last_name = name_parts[-1][:3].upper()
        base_id = f"{first_name}{last_name}"
    else:
        # Single name fallback
        base_id = f"{driver_name[:6].upper().replace(' ', '')}"
    
    # Collect all existing IDs from all sources
    all_existing_ids = set()
    for source in existing_ids_sources:
        if isinstance(source, dict):
            # For dictionaries, add all keys (for missing_drivers) or all values (for id maps)
            if source:
                first_key = next(iter(source.keys()))
                if isinstance(source[first_key], dict) and 'driver_id' in source[first_key]:
                    # This is a dimension-like dict, extract driver_ids
                    all_existing_ids.update(item['driver_id'] for item in source.values())
                else:
                    # This is probably an id map, add values
                    all_existing_ids.update(source.values())
        elif hasattr(source, '__iter__'):
            # For iterables (like driver_id_map.values())
            all_existing_ids.update(source)
    
    # Find next available number
    counter = 1
    while True:
        driver_id = f"{base_id}{counter:02d}"
        if driver_id not in all_existing_ids:
            return driver_id
        counter += 1
        if counter > 99:  # Safety check
            return f"{base_id}{counter}"  # No leading zero for 3+ digits


def find_driver_id(driver_name, year, driver_cache, dimensions, missing_drivers):
    # Create cache key - include year for drivers with multiple entries
    cache_key = f"{driver_name.lower()}|{year}"

    # Check cache first
    if cache_key in driver_cache:
        return driver_cache[cache_key]

    driver_id = None
    normalized_name = normalize_driver_name(driver_name)

    # Special handling for drivers with multiple entries based on era/year
    if driver_name.lower() == "nelson piquet":
        if int(year) <= 1991:
            # Look for NELPIQ01 first (Sr.)
            for d_id, d_info in dimensions['drivers'].items():
                if d_info['driver_name'].lower() == driver_name.lower() and "01" in d_id:
                    driver_id = d_id
                    break
        else:
            # Look for NELPIQ02 (Jr.)
            for d_id, d_info in dimensions['drivers'].items():
                if d_info['driver_name'].lower() == driver_name.lower() and "02" in d_id:
                    driver_id = d_id
                    break
    
    elif driver_name.lower() == "robert doornbos":
        if int(year) == 2005:
            # Look for ROBDOO02 first (2005 Monaco)
            for d_id, d_info in dimensions['drivers'].items():
                if d_info['driver_name'].lower() == driver_name.lower() and "02" in d_id:
                    driver_id = d_id
                    break
        else:
            # Look for ROBDOO02 (2006+ Netherlands)
            for d_id, d_info in dimensions['drivers'].items():
                if d_info['driver_name'].lower() == driver_name.lower() and "01" in d_id:
                    driver_id = d_id
                    break
    
    else:
        # Regular matching - just pick the first match found
        for d_id, d_info in dimensions['drivers'].items():
            if normalize_driver_name(d_info['driver_name']) == normalized_name:
                driver_id = d_id
                break
                
        # Also check in missing_drivers
        if not driver_id:
            for d_id, d_info in missing_drivers.items():
                if normalize_driver_name(d_info['driver_name']) == normalized_name:
                    driver_id = d_id
                    break

    # Create new entry if still no match
    if not driver_id:
        driver_id = generate_unique_driver_id(
            driver_name, 
            [dimensions['drivers'], missing_drivers]
        )
        
        missing_drivers[driver_id] = {
            'driver_id': driver_id,
            'driver_name': normalize_driver_name(driver_name)
        }
    
    driver_cache[cache_key] = driver_id
    return driver_id
    
def normalize_driver_name(name):
    """Standardize driver name format for consistent matching"""
    if not name:
        return ""
    # Remove extra spaces, standardize case
    return " ".join(name.strip().split())
