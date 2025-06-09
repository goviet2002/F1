from datetime import datetime
import certifi
import aiohttp
import ssl

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