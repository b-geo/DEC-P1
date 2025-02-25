import pandas as pd

def transform_odds(resp: dict) -> pd.DataFrame:
    all_bookmakers = {
        bookmaker.get("title")
        for game in resp
        for bookmaker in game.get("bookmakers", [])
    }
    print(all_bookmakers)
    df = pd.json_normalize(resp)
    df = df.drop(columns=["sport_key", "sport_title"])
