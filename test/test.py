from unittest import result
import requests
from bs4 import BeautifulSoup
import pandas as pd
from torch import res

urls = "https://www.formula1.com/en/results/2024/races/1229/bahrain/fastest-laps"
head = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}

races = ['bahrain', 'saudi-arabia', 'australia']
race_with_no = []
race_no = urls.split("/")[7]

counter = 0
for race in races:
    race_with_no.append((int(race_no) + counter, race))
    counter += 1

data = []
headers = []
for (no, city) in race_with_no:
    url = f"https://www.formula1.com/en/results/2024/races/{no}/{city}/fastest-laps"
    reponse = requests.get(url)
    html = reponse.text
    soup = BeautifulSoup(html, 'html.parser')

    table = soup.find('table', class_='f1-table-with-data')
    city = city.replace("-", " ").title()
    
    if len(headers) == 0:
        for header in table.find('thead').find_all('th'):
            each = header.text.strip()
            headers.append(each) 
            
    rows = table.find('tbody').find_all('tr')

    for row in rows:
        cols= row.find_all('td')
        rows_data = []

        for index, col in enumerate(cols):
            if index == 2:
                info = col.text.strip().replace("\xa0", " ")[:-3]
                rows_data.append(info)
            else:
                info = col.text.strip()
                rows_data.append(info)
        rows_data.append(city)
        data.append(rows_data)
        
headers.append("City")
result = pd.DataFrame(data, columns=headers)
result.to_csv("fastest_laps.csv", index=False)
print(result)


