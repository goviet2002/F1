import os
import json
from collections import defaultdict
import re
import datetime
from tracemalloc import start

# Base data directory
DATA_DIR = r"C:\Users\anhvi\OneDrive\Desktop\F1 Projekt\data\f1_data"

for year_dir in os.listdir(DATA_DIR):
    year_path = os.path.join(DATA_DIR, year_dir)
    for gp_dir in os.listdir(year_path):
        gp_path = os.path.join(year_path, gp_dir)

                
        grand_prix = gp_dir
        print(f"Processing {grand_prix} in {year_dir}")