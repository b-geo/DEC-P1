import pytest
import pandas as pd
from main.assets.helpers import compare_table_schema, current_round
from sqlalchemy import Table, MetaData, Column, Integer, String, Float

@pytest.fixture(scope="session")
def source_table():
    metadata = MetaData()
    table = Table(
    "games",
    metadata,
    Column("game_id", Integer, primary_key=True),
    Column("game_data_updated", String),
    Column("game_time", String),
    Column("home_team", String),
    Column("away_team", String),
    Column("round", Integer),
    Column("complete_perc", Integer)
    )
    return table

@pytest.fixture(scope="session")
def target_table():
    metadata = MetaData()
    table = Table(
    "games",
    metadata,
    Column("game_id", Integer, primary_key=True),
    Column("game_data_updated", String),
    Column("game_time", String),
    Column("home_team", String),
    Column("away_team", String),
    Column("round", Integer),
    )
    return table

@pytest.fixture(scope="session")
def target_table2():
    metadata = MetaData()
    table = Table(
    "games",
    metadata,
    Column("game_id", Integer, primary_key=True),
    Column("game_data_updated", String),
    Column("game_time", String),
    Column("home_team", String),
    Column("away_team", String),
    Column("round", Integer),
    Column("complete_perc", String)
    )
    return table

@pytest.fixture(scope="session")
def games_df():
    return pd.json_normalize(
        {
        "game_id": 37090,
        "game_data_updated": "2024-11-14 17:08:26",
        "game_time": "2025-03-06 18:50:00",
        "home team": "Brisbane Lions",
        "away team": "Geelong",
        "complete_perc": 0,
        "round": 0
        }
    )

# Test the correct exceptions are raised when two tables aren't the same.
def test_compare_table_schema(source_table, target_table, target_table2):
    with pytest.raises(Exception, match="Table games in target DB does not contain column complete_perc."):
        compare_table_schema(source_table = source_table, target_table = target_table)
    
    with pytest.raises(Exception, match=f"Source column 'complete_perc' is INTEGER but expected VARCHAR."):
        compare_table_schema(source_table = source_table, target_table = target_table2)

 #Test that based on a static data set, the correct round is computed.   
def test_current_round(games_df):
    expected_round = 0
    actual_round = current_round(games_df)
    assert actual_round == expected_round
        