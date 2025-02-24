import requests
import pandas as pd

def extract(api_key: str) -> list:
    """
    Args: api_key as string
    Returns: JSON of AFL Games and odds for each bookmaker
    Raises: Status code if not 200 and message
    """
    resp = requests.get("https://api.the-odds-api.com/v4/sports/aussierules_afl/odds", params={
                "api_key": api_key,
                "regions": "au",
                "markets": "h2h",
                "oddsFormat": "decimal",
                "dateFormat": "iso",
            }, timeout=5)
    if resp.status_code == 200:
        print(resp.json())
        return resp.json()
    else: return [{"Error": resp.status_code}]

