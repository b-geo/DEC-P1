import datetime
from main.connectors.squiggle_api import SquiggleApiClient
from main.connectors.postgresql import PostgreSqlClient
from main.assets.load import load_to_postgres
from main.assets.extract import extract_games, extract_odds, extract_player_rankings, extract_postgres
from main.assets.transform import transform_games, transform_odds, transform_player_rankings
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String
from sqlalchemy.engine import Engine
from jinja2 import Environment, Template, FileSystemLoader

# from dotenv import load_dotenv
# import os
# import yaml
# from pathlib import Path
# import schedule
# import time
# from importlib import import_module

this_year = datetime.date.today().year

print(this_year)


a = SquiggleApiClient("brody1geroge@gmail.com")
b = extract_games(a,this_year)
c = transform_games(b)
# # b = b.loc[b['complete'] == 0]
# current_round = b["round"].iloc[0]
# print(extract_odds(a,this_year,current_round))

# print(transform_player_rankings(extract_player_rankings("./main/data/raw/2024-r24-player_rankings.csv")))
load_dotenv()

TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
TARGET_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
TARGET_PORT = os.environ.get("TARGET_PORT")

metadata = MetaData()
table = Table(
"games",
metadata,
Column("game_id", Integer, primary_key=True),
Column("game_data_updated", String),
Column("game_time", String),
Column("home_team", String),
Column("away_team", String),
Column("round", Integer)
)

client = PostgreSqlClient(TARGET_SERVER_NAME, TARGET_DATABASE_NAME, TARGET_DB_USERNAME, TARGET_DB_PASSWORD, TARGET_PORT)
load_to_postgres(c,client, table, metadata)

environment = Environment(loader=FileSystemLoader("./main/assets/sql/extract"))
for sql_path in environment.list_templates():
    sql_template = environment.get_template(sql_path)
    print(extract_postgres(sql_template=sql_template,engine=client.engine,where="round = 0"))
