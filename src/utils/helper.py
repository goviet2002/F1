
import itertools

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

# Helper function to safely convert to float
def safe_float(value):
    """Safely convert value to float, return None if conversion fails"""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
