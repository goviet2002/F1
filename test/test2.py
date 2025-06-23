import aiohttp
from bs4 import BeautifulSoup
import asyncio
import re

head = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

async def get_dropdown_sessions(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=head) as resp:
            html = await resp.text()
            soup = BeautifulSoup(html, "lxml")
            dropdown = soup.find_all("a", class_="DropdownMenuItem-module_dropdown-menu-item__6Y3-v")
            sessions = []
            m = re.search(r"(/races/\d+/[a-z0-9\-]+)/", url)
            race_path = m.group(1) if m else None
            for item in dropdown:
                session_name = item.get_text(strip=True).replace("Active", "").strip()
                session_url = item.get("href")
                # Filter out links with "Flag of" in the name
                if race_path and session_url and race_path in session_url and "Flag of" not in session_name:
                    sessions.append((session_name, f"https://www.formula1.com{session_url}"))
            return sessions

if __name__ == "__main__":
    url = "https://www.formula1.com/en/results/1992/races/575/mexico/race-result"
    sessions = asyncio.run(get_dropdown_sessions(url))
    print(sessions)