import pandas as pd
from pathlib import Path
from main.connectors.squiggle_api import SquiggleApiClient
from main.connectors.postgresql import PostgreSqlClient
from jinja2 import Template

def extract_games(squiggle_api_client: SquiggleApiClient, year: int) -> pd.DataFrame:
    """Extracts game details from Squiggle API.

    Args:
        squiggle_api_client (SquiggleApiClient): A Squiggle API client to interface.
        year (int): The season (year) games data is to be extracted for.

    Returns:
        pd.DataFrame: Contains games as rows.
    """
    games_data = squiggle_api_client.get_games(year=year)
    df_games = pd.json_normalize(games_data["games"])
    return df_games

def extract_odds(squiggle_api_client: SquiggleApiClient, year: int, round: str) -> pd.DataFrame:
    """Extracts odds details from Squiggle API.

    Args:
        squiggle_api_client (SquiggleApiClient): A Squiggle API client to interface.
        year (int): The season (year) games data is to be extracted for.
        year (int): The round games data is to be extracted for.

    Returns:
        pd.DataFrame: Contains games with odds details as rows.
    """
    odds_data = squiggle_api_client.get_odds(year=year, round=round)
    df_odds = pd.json_normalize(odds_data["tips"])
    return df_odds

def extract_player_rankings(player_rankings_path: Path) -> pd.DataFrame:
    """ Extracts player fantasy data from CSV.

    Args:
        player_rankings_path (Path): The location of the player rankings CSV from Footywire.

    Raises:
        Exception: Missing player_rankings_path file for player rankings data!

    Returns:
        pd.DataFrame: Contains player ranking details as rows.
    """
    if Path(player_rankings_path).exists(): # pd would have better exception handling than me but oh well.
        df_player_rankings = pd.read_csv(player_rankings_path)
        return df_player_rankings
    else:
        raise Exception(
            f"Missing {player_rankings_path} file for player rankings data!"
        )

# It would probably make more sense for the PostgresSQL class to handle this but
# I have kept it in the extract module for consistency with the other extract functions.
def extract_postgres(postgres_client: PostgreSqlClient, sql_template: Template) -> list[dict]:
    """ Extracts data from PostgreSQL database based on a SQL query.

    Args:
        postgres_client (PostgreSqlClient): A PostgreSQL client to connect to the database.
        sql_template (Template): A SQL file containing the query.

    Returns:
        list[dict]: A list of rows from the query result.
    """
    sql = sql_template.render()
    return [dict(row) for row in postgres_client.engine.execute(sql).all()]
