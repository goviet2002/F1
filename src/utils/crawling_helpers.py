from datetime import datetime
import certifi
import aiohttp
import ssl
import unicodedata

ssl_context = ssl.create_default_context(cafile=certifi.where())

head = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
base_url = "https://www.formula1.com"

# Get years that statistics have been published
current_year = datetime.now().year
years = [year for year in range(1950, current_year + 1)]

async def test_function(param, functions):
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        result = await functions(session, param)
        return result
    
def standardize_folder_name(name):
    """Convert any name to a consistent folder name format"""
    # Normalize Unicode characters
    folder_name = unicodedata.normalize('NFKD', name)
    # Remove non-ASCII characters
    folder_name = ''.join([c for c in folder_name if not unicodedata.combining(c)])
    # Convert to lowercase
    folder_name = folder_name.lower()
    # Replace special characters
    folder_name = folder_name.replace("'", "")
    folder_name = folder_name.replace("-", "_")
    folder_name = folder_name.replace(" ", "_")
    return folder_name