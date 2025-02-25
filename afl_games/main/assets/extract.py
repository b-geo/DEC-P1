import pandas as pd
from pathlib import Path
from main.connectors.squiggle_api import SquiggleApiClient
from sqlalchemy.engine import Engine
from jinja2 import Template

def extract_games(squiggle_api_client: SquiggleApiClient, year: str) -> pd.DataFrame:
    games_data = squiggle_api_client.get_games(year=year)
    df_games = pd.json_normalize(games_data["games"])
    return df_games

def extract_odds(squiggle_api_client: SquiggleApiClient, year: str, round: str) -> pd.DataFrame:
    odds_data = squiggle_api_client.get_odds(year=year, round=round)
    df_odds = pd.json_normalize(odds_data["tips"])
    return df_odds

def extract_player_rankings(player_rankings_path: Path) -> pd.DataFrame:
    df_player_rankings = pd.read_csv(player_rankings_path)
    return df_player_rankings

def extract_postgres(
    sql_template: Template,
    engine: Engine,
    where: str
) -> list[dict]:
    extract_type = sql_template.make_module().config.get("extract_type")
    if extract_type == "full":
        if where:
            sql = sql_template.render(optional_where=where)
        else:
            sql = sql_template.render()
        return [dict(row) for row in engine.execute(sql).all()]
    else:
        raise Exception(
            f"Extract type {extract_type} is not supported. Please use either 'full' or 'incremental' extract type."
        )
