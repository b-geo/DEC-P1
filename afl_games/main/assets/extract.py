import pandas as pd
from pathlib import Path
from main.connectors.squiggle_api import SquiggleApiClient
from main.connectors.postgresql import PostgreSqlClient
from sqlalchemy.engine import Engine
from jinja2 import Template

def extract_games(squiggle_api_client: SquiggleApiClient, year: int) -> pd.DataFrame:
    games_data = squiggle_api_client.get_games(year=year)
    df_games = pd.json_normalize(games_data["games"])
    return df_games

def extract_odds(squiggle_api_client: SquiggleApiClient, year: int, round: str) -> pd.DataFrame:
    odds_data = squiggle_api_client.get_odds(year=year, round=round)
    df_odds = pd.json_normalize(odds_data["tips"])
    return df_odds

def extract_player_rankings(player_rankings_path: Path) -> pd.DataFrame:
    df_player_rankings = pd.read_csv(player_rankings_path)
    return df_player_rankings

# this function would normally be if changing the method of extract but have kept for consistency with the above
# even though we could just use the postgres class instance to execute this.
def extract_postgres(postgres_client: PostgreSqlClient, sql_template: Template) -> list[dict]:
    """extract will only ever be full so I have removed the other extract options"""
    sql = sql_template.render()
    return [dict(row) for row in postgres_client.engine.execute(sql).all()]
