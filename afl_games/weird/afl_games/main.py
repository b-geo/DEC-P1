"""
- extract via both APIs
- transform in pandas
- upload to db

- if _main_
- doc strings
- type hints

- using GIT
- google"s python style guide
- navigate using keyboard 
    - crtl + arrow for moving between words
    - alt up or down to move line up or down
    - ctrl enter to insert line below 
    - ctrl tab between working files
    - ctrl end to end of file
- separate into objects
- separate into files

- log
- unit test
- schedule
"""
import os
import dotenv
from extract.odds import extract as odds_api

# team names odds api : squiggle api
name_dict = {"Adelaide Crows": "Adelaide",
            "Brisbane Lions": "Brisbane Lions",
            "Carlton Blues": "Carlton",
            "Collingwood Magpies": "Collingwood",
            "Essendon Bombers": "Essendon",
            "Fremantle Dockers": "Fremantle",
            "Geelong Cats": "Geelong",
            "Gold Coast Suns": "Gold Coast",
            "Greater Western Sydney Giants":
            "Greater Western Sydney",
            "Hawthorn Hawks": "Hawthorn",
            "Melbourne Demons": "Melbourne",
            "North Melbourne Kangaroos":"North Melbourne",
            "Port Adelaide Power": "Port Adelaide",
            "Richmond Tigers": "Richmond",
            "St Kilda Saints": "St Kilda",
            "Sydney Swans": "Sydney",
            "West Coast Eagles": "West Coast",
            "Western Bulldogs": "Western Bulldogs"
            }


if __name__ == "__main__":
    dotenv.load_dotenv()
    ODDS_API_KEY = os.environ.get("ODDS_API_KEY")
    odds_data = odds_api(ODDS_API_KEY)