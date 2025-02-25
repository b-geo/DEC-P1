import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

s = requests.Session()

retries = Retry(total=5,
                status_forcelist=[429, 500, 502, 503, 504])

s.mount('http://', HTTPAdapter(max_retries=retries))

resp = s.get('https://api.squiggle.com.au/?q=games;year=2024',headers={"User-Agent": "brody1geore@gmail.com"})
print(resp.json())

# Name formats used for data sources: {Squiggle API: Rankings CSV}
{'Adelaide': 'Crows',
 'Brisbane Lions': 'Lions',
 'Carlton': 'Blues',
 'Collingwood': 'Magpies',
 'Essendon': 'Bombers',
 'Fremantle': 'Dockers',
 'Geelong': 'Cats',
 'Gold Coast': 'Suns',
 'Greater Western Sydney': 'Giants',
 'Hawthorn': 'Hawks',
 'Melbourne': 'Demons',
 'North Melbourne': 'Kangaroos',
 'Port Adelaide': 'Power',
 'Richmond': 'Tigers',
 'St Kilda': 'Saints',
 'Sydney': 'Swans',
 'West Coast': 'Eagles',
 'Western Bulldogs': 'Bulldogs'}

