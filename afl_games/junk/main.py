import datetime
from main.connectors.squiggle_api import SquiggleApiClient
from main.connectors.postgresql import PostgreSqlClient
from main.assets.load import PostgresSqlLoader
from main.assets.helpers import compare_table_schema, current_round
from main.assets.extract import extract_games, extract_odds, extract_player_rankings, extract_postgres
from main.assets.transform import transform_games, transform_odds, transform_player_rankings
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float
from sqlalchemy.engine import Engine
from jinja2 import Environment, Template, FileSystemLoader
import pandas as pd

# from dotenv import load_dotenv
# import os
# import yaml
# from pathlib import Path
# import schedule
# import time
# from importlib import import_module

this_year = datetime.date.today().year



squiggle_client = SquiggleApiClient("brody1geroge@gmail.com")
games_data = extract_games(squiggle_client,this_year)
games_df = transform_games(games_data)

current_round = current_round(games_df)
odds_data = extract_odds(squiggle_client,this_year,current_round)
odds_df = transform_odds(odds_data)

team_fantasy_data = extract_player_rankings("./main/data/raw/2024-r24-player_rankings.csv")
team_fantasy_df = transform_player_rankings(team_fantasy_data)

#---

# # b = b.loc[b['complete'] == 0]
# current_round = b["round"].iloc[0]
# print(extract_odds(a,this_year,current_round))

# print(transform_player_rankings(extract_player_rankings("./main/data/raw/2024-r24-player_rankings.csv")))

#PASSED IN AT MAIN TO POSTGRES CLIENT NOT LOAD
load_dotenv()

TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
TARGET_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
TARGET_PORT = os.environ.get("TARGET_PORT")

SOURCE_DATABASE_NAME = os.environ.get("SOURCE_DATABASE_NAME")
SOURCE_SERVER_NAME = os.environ.get("SOURCE_SERVER_NAME")
SOURCE_DB_USERNAME = os.environ.get("SOURCE_DB_USERNAME")
SOURCE_DB_PASSWORD = os.environ.get("SOURCE_DB_PASSWORD")
SOURCE_PORT = os.environ.get("SOURCE_PORT")

target_postgres_client = PostgreSqlClient(TARGET_SERVER_NAME, TARGET_DATABASE_NAME, TARGET_DB_USERNAME, TARGET_DB_PASSWORD, TARGET_PORT)
source_postgres_client = PostgreSqlClient(SOURCE_SERVER_NAME, SOURCE_DATABASE_NAME, SOURCE_DB_USERNAME, SOURCE_DB_PASSWORD, SOURCE_PORT)
#make into loading class
#make into loading class
games_loader = PostgresSqlLoader(target_postgres_client)
games_loader.load_games(df=games_df, load_method="upsert")

odds_loader = PostgresSqlLoader(target_postgres_client)
odds_loader.load_odds(df=odds_df, load_method="overwrite")

team_fantasy_loader = PostgresSqlLoader(target_postgres_client)
team_fantasy_loader.load_team_fantasy(df=team_fantasy_df, load_method="upsert")

# #yaml the below directory?
environment = Environment(loader=FileSystemLoader("./main/assets/sql/extract"))
summary_template = environment.get_template("summary.sql")
summary_data = extract_postgres(postgres_client=source_postgres_client, sql_template=summary_template)
summary_df = pd.json_normalize(summary_data)

# summary_loader = PostgresSqlLoader(target_postgres_client)
# summary_loader.load_games(df=summary_df, load_method="overwrite")

#
# template_list = [environment.get_template(sql_path) for sql_path in environment.list_templates()]
# for template in template_list:
#     print(extract_postgres(postgres_client=postgres_client, sql_template=template))

# for sql_path in environment.list_templates():
#     print(sql_path)