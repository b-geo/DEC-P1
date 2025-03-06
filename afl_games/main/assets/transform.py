import pandas as pd

def transform_games(df: pd.DataFrame) -> pd.DataFrame:
    """Transform games data to be more human readable and remove excess data."""
    # Concatenate time and timezone of the game to a single column.
    df.loc[:,"localtime"] = df["localtime"] + df["tz"]
    # Sort rows by `unixtime`.
    df_sorted = df.sort_values(by=["unixtime"], ascending=True)
    # Select a subset of columns.
    df_selected = df_sorted.loc[:, ["id", "updated", "localtime", "hteam", "ateam", "round", "complete"]]
    # Cast `id`, `round` and `complete` as numeric types.
    df_selected.loc[:,"id"]= pd.to_numeric(df_selected.loc[:,"id"])
    df_selected.loc[:,"round"]= pd.to_numeric(df_selected.loc[:,"round"])
    df_selected.loc[:,"complete"]= pd.to_numeric(df_selected.loc[:,"complete"])
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
    """Transform odds data to be more human readable and remove excess data."""
    # Select a subset of columns
    df_selected = df.loc[:, ["gameid", "updated", "tip", "margin", "confidence"]]
    # Cast `gameid`, `margin` and `confidence` as numeric types.
    df_selected.loc[:,"margin"]= pd.to_numeric(df_selected.loc[:,"margin"])
    df_selected.loc[:,"gameid"]= pd.to_numeric(df_selected.loc[:,"gameid"])
    df_selected.loc[:,"confidence"]= pd.to_numeric(df_selected.loc[:,"confidence"])
    # All but "tip" and "confidence" renamed
    df_renamed = df_selected.rename(
        columns={
            "gameid": "game_id",
            "updated": "odds_data_updated",
            "margin": "projected_margin"
        }
    )
    return df_renamed

def transform_player_rankings(df: pd.DataFrame) -> pd.DataFrame:
    """Accepts individual player fantasy data and transforms data to be grouped by team."""
    # A dictionary of team names in player rankings data and their equivalent with Squiggle API
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
    # A mapping function is applied to the `teams` column to team names in this dataset to match Squiggle team names.
    df.loc[:,["Team"]] = df["Team"].map(teams_names_mapping).fillna(df["Team"])
    # Individual players are grouped by team.
    df_grouped = df.groupby(by=["Team"])["Score"].sum().reset_index()
    df_selected = df_grouped.loc[:,["Team","Score"]]
    # Cast `Score` as a numeric type.
    df_selected.loc[:,"Score"]= pd.to_numeric(df_selected.loc[:,"Score"])
    # Sort rows by `Score`.
    df_sorted = df_selected.sort_values(by=["Score"], ascending=False)
    # All columns are renamed.
    df_renamed = df_sorted.rename(
        columns={
            "Team": "team",
            "Score": "team_fantasy_score",
        }
    )
    return df_renamed
