import pandas as pd

def transform_games(df: pd.DataFrame) -> pd.DataFrame:
    df["localtime"] = df["localtime"] + df["tz"]
    df_sorted = df.sort_values(by=["unixtime"], ascending=True)
    df_selected = df_sorted[["id", "updated", "localtime", "hteam", "ateam", "round", "complete"]]
    df_selected.loc[:,"id"]= pd.to_numeric(df_selected["id"])
    df_selected.loc[:,"round"]= pd.to_numeric(df_selected["round"])
    df_selected.loc[:,"complete"]= pd.to_numeric(df_selected["complete"])
    # All but "round" renamed
    df_renamed = df_selected.rename(
        columns={
            "id": "game_id",
            "updated": "game_data_updated",
            "localtime": "game_time",
            "hteam": "home_team",
            "ateam": "away_team",
            "complete": "complete_perc"
        }
    )
    return df_renamed

def transform_odds(df: pd.DataFrame) -> pd.DataFrame:
    df_selected = df[["gameid", "updated", "tip", "margin", "confidence"]]
    df_selected.loc[:,"margin"]= pd.to_numeric(df_selected["margin"])
    df_selected.loc[:,"gameid"]= pd.to_numeric(df_selected["gameid"])
    # "tip" and "confidence" not renamed
    df_renamed = df_selected.rename(
        columns={
            "gameid": "game_id",
            "updated": "odds_data_updated",
            "margin": "projected_margin"
        }
    )
    return df_renamed

def transform_player_rankings(df: pd.DataFrame) -> pd.DataFrame:
    teams_names_mapping = {
        "Crows": "Adelaide",
        "Lions": "Brisbane Lions",
        "Blues": "Carlton",
        "Magpies": "Collingwood",
        "Bombers": "Essendon",
        "Dockers": "Fremantle",
        "Cats": "Geelong",
        "Suns": "Gold Coast",
        "Giants": "Greater Western Sydney",
        "Hawks": "Hawthorn",
        "Demons": "Melbourne",
        "Kangaroos": "North Melbourne",
        "Power": "Port Adelaide",
        "Tigers": "Richmond",
        "Saints": "St Kilda",
        "Swans": "Sydney",
        "Eagles": "West Coast",
        "Bulldogs": "Western Bulldogs"}
    df["Team"] = df["Team"].map(teams_names_mapping).fillna(df["Team"])
    df_grouped = df.groupby(by=["Team"])["Score"].sum().reset_index()
    df_selected = df_grouped[["Team","Score"]]
    df_selected.loc[:,"Score"]= pd.to_numeric(df_selected["Score"])
    df_sorted = df_selected.sort_values(by=["Score"], ascending=False)
    df_renamed = df_sorted.rename(
        columns={
            "Team": "team",
            "Score": "team_fantasy_score",
        }
    )
    return df_renamed
