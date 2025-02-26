import datetime
from main.connectors.squiggle_api import SquiggleApiClient
from main.connectors.postgresql import PostgreSqlClient
from main.assets.load import PostgresSqlLoader
from main.assets.helpers import current_round
from main.assets.extract import extract_games, extract_odds, extract_player_rankings, extract_postgres
from main.assets.transform import transform_games, transform_odds, transform_player_rankings
from dotenv import load_dotenv
import os
from jinja2 import Environment, FileSystemLoader
import pandas as pd
from loguru import logger
import sys

#use tr

def run_pipeline(pipeline_config):
    logger.remove(0)
    # logger.add(sys.stderr, format="{time} | {level} | {message}")
    logger.add(f"{pipeline_config.get("log_folder_path")}\\file_name.log", retention="1 minute", format="{time} | {level} | {message}")
    load_dotenv()
    #source db
    SOURCE_DATABASE_NAME = os.environ.get("SOURCE_DATABASE_NAME")
    SOURCE_SERVER_NAME = os.environ.get("SOURCE_SERVER_NAME")
    SOURCE_DB_USERNAME = os.environ.get("SOURCE_DB_USERNAME")
    SOURCE_DB_PASSWORD = os.environ.get("SOURCE_DB_PASSWORD")
    SOURCE_PORT = os.environ.get("SOURCE_PORT")
    logger.info(f"Creating source client for {SOURCE_DATABASE_NAME} database.")
    source_postgres_client = PostgreSqlClient(SOURCE_SERVER_NAME, SOURCE_DATABASE_NAME, SOURCE_DB_USERNAME, SOURCE_DB_PASSWORD, SOURCE_PORT)
    #target db
    TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
    TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
    TARGET_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
    TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
    TARGET_PORT = os.environ.get("TARGET_PORT")
    logger.info(f"Creating target client for {TARGET_DATABASE_NAME} database.")
    target_postgres_client = PostgreSqlClient(TARGET_SERVER_NAME, TARGET_DATABASE_NAME, TARGET_DB_USERNAME, TARGET_DB_PASSWORD, TARGET_PORT)

    logger.info(f"Creating Squiggle API client.")
    squiggle_client = SquiggleApiClient("brody1geroge@gmail.com")
    #get what year we're in to know what AFL season we are in
    this_year = datetime.date.today().year
    #extract game data
    logger.info(f"Extracting games data for season {this_year}.")
    games_data = extract_games(squiggle_api_client=squiggle_client, year=this_year)
    #transform game data
    logger.info(f"Transforming games data.")
    games_df = transform_games(games_data)

    #get what round we're in so that we only get games from the current round
    round = current_round(games_df)
    #extract odds data
    logger.info(f"Extracting odds data for season {this_year}, round {round}.")
    odds_data = extract_odds(squiggle_client,this_year,round)
    #transform odds data
    logger.info(f"Transforming odds data.")
    odds_df = transform_odds(odds_data)

    #extract team fantasy data
    logger.info(f"Extracting player fantasy scoring data.")
    team_fantasy_data = extract_player_rankings(pipeline_config.get("player_rankings_path"))
    #transform team fantasy data
    logger.info(f"Transforming player fantasy data to group by team.")
    team_fantasy_df = transform_player_rankings(team_fantasy_data)

    #load games data to postgres db
    logger.info(f"Loading games data to {TARGET_DATABASE_NAME}.")
    games_loader = PostgresSqlLoader(target_postgres_client)
    games_loader.load_games(df=games_df, load_method="upsert")

    #load odds data to postgres db
    logger.info(f"Loading odds data to {TARGET_DATABASE_NAME}.")
    odds_loader = PostgresSqlLoader(target_postgres_client)
    odds_loader.load_odds(df=odds_df, load_method="overwrite")

    #load team fantasy data to postgres db
    logger.info(f"Loading team fantasy data to {TARGET_DATABASE_NAME}.")
    team_fantasy_loader = PostgresSqlLoader(target_postgres_client)
    team_fantasy_loader.load_team_fantasy(df=team_fantasy_df, load_method="upsert")

    #sql query target db and return as df
    logger.info(f"Getting SQL files from sql/extract.")
    environment = Environment(loader=FileSystemLoader(pipeline_config.get("sql_extract_path")))
    logger.info(f"Extracting summary query data from {SOURCE_DATABASE_NAME}.")
    summary_template = environment.get_template("summary.sql")
    summary_data = extract_postgres(postgres_client=source_postgres_client, sql_template=summary_template)
    summary_df = pd.json_normalize(summary_data)

    #load summary data to postgres db
    logger.info(f"Loading team fantasy data to {TARGET_DATABASE_NAME}.")
    summary_loader = PostgresSqlLoader(target_postgres_client)
    summary_loader.load_summary(df=summary_df, load_method="overwrite")
