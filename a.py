import requests
import time
import certifi

# Using requests.get()
start_time = time.time()
for _ in range(10):
    response = requests.get('https://www.formula1.com/en/results/2025/races',verify=certifi.where())
print("requests.get() time:", time.time() - start_time)

# Using requests.Session()
session = requests.Session()
start_time = time.time()
for _ in range(10):
    response = session.get('https://www.formula1.com/en/results/2025/races', verify=certifi.where())
print("requests.Session() time:", time.time() - start_time)
