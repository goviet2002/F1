
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
        nationality_code = standing['nationality_code']
        nationality_full = get_full_nationality(nationality_code)
        
        if driver_id and driver_id not in id_to_nationality:
            id_to_nationality[driver_id] = {
                'nationality_code': nationality_code,
                'nationality': nationality_full
            }
    
    # Update driver dimensions with both code and full name
    for driver_id, driver_info in dimensions['drivers'].items():
        if driver_id in id_to_nationality:
            nat_info = id_to_nationality[driver_id]
            driver_info['nationality_code'] = nat_info['nationality_code']
            driver_info['nationality'] = nat_info['nationality']

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

