import datetime
import os
import pandas as pd
from main.connectors.squiggle_api import SquiggleApiClient
from main.connectors.postgresql import PostgreSqlClient
from main.assets.load import PostgresSqlLoader
from main.assets.helpers import current_round
from main.assets.extract import extract_games, extract_odds, extract_player_rankings, extract_postgres
from main.assets.transform import transform_games, transform_odds, transform_player_rankings
from main.assets.logging import PipelineLogging, MetaDataLogging, MetaDataLoggingStatus
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader


def run(pipeline_name: str, pipeline_config: dict, postgres_logging_client: PostgreSqlClient):   
    #set up logging
    pipeline_logging = PipelineLogging(
        pipeline_name = pipeline_name,
        log_folder_path = pipeline_config.get("log_folder_path")
        )
    logger = pipeline_logging.logger
    metadata_logging = MetaDataLogging(
    pipeline_name = pipeline_name,
    postgresql_client = postgres_logging_client,
    config = pipeline_config
    )
    try:
        #load env variables
        load_dotenv()
        
        #silver db
        SILVER_DATABASE_NAME = os.environ.get("SILVER_DATABASE_NAME")
        if len(SILVER_DATABASE_NAME) < 1:
            pipeline_logging.logger.warning(f"SILVER_DATABASE_NAME is empty.")
        SILVER_SERVER_NAME = os.environ.get("SILVER_SERVER_NAME")
        if len(SILVER_SERVER_NAME) < 1:
            pipeline_logging.logger.warning(f"SILVER_SERVER_NAME is empty.")
        SILVER_DB_USERNAME = os.environ.get("SILVER_DB_USERNAME")
        if len(SILVER_DB_USERNAME) < 1:
            pipeline_logging.logger.warning(f"SILVER_DB_USERNAME is empty.")
        SILVER_DB_PASSWORD = os.environ.get("SILVER_DB_PASSWORD")
        if len(SILVER_DB_PASSWORD) < 1:
            pipeline_logging.logger.warning(f"SILVER_DB_PASSWORD is empty.")
        SILVER_PORT = os.environ.get("SILVER_PORT")
        if len(SILVER_PORT) < 1:
            pipeline_logging.logger.warning(f"SILVER_PORT is empty.")
        logger.info(f"Creating client for {SILVER_DATABASE_NAME} database (silver layer).")
        silver_postgres_client = PostgreSqlClient(SILVER_SERVER_NAME, SILVER_DATABASE_NAME, SILVER_DB_USERNAME, SILVER_DB_PASSWORD, SILVER_PORT)
        
        #gold db
        GOLD_DATABASE_NAME = os.environ.get("GOLD_DATABASE_NAME")
        if len(GOLD_DATABASE_NAME) < 1:
            pipeline_logging.logger.warning(f"GOLD_DATABASE_NAME is empty.")
        GOLD_SERVER_NAME = os.environ.get("GOLD_SERVER_NAME")
        if len(GOLD_SERVER_NAME) < 1:
            pipeline_logging.logger.warning(f"GOLD_SERVER_NAME is empty.")
        GOLD_DB_USERNAME = os.environ.get("GOLD_DB_USERNAME")
        if len(GOLD_DB_USERNAME) < 1:
            pipeline_logging.logger.warning(f"GOLD_DB_USERNAME is empty.")
        GOLD_DB_PASSWORD = os.environ.get("GOLD_DB_PASSWORD")
        if len(GOLD_DB_PASSWORD) < 1:
            pipeline_logging.logger.warning(f"GOLD_DB_PASSWORD is empty.")
        GOLD_PORT = os.environ.get("GOLD_PORT")
        if len(GOLD_PORT) < 1:
            pipeline_logging.logger.warning(f"GOLD_PORT is empty.")
        logger.info(f"Creating client for {GOLD_DATABASE_NAME} database (gold layer).")
        gold_postgres_client = PostgreSqlClient(GOLD_SERVER_NAME, GOLD_DATABASE_NAME, GOLD_DB_USERNAME, GOLD_DB_PASSWORD, GOLD_PORT)
        
        #squiggle user agent
        SQUIGGLE_USER_AGENT = os.environ.get("SQUIGGLE_USER_AGENT")
        
        #log start to db
        metadata_logging.log()
        
        logger.info(f"Creating Squiggle API client.")
        squiggle_client = SquiggleApiClient(SQUIGGLE_USER_AGENT)
        #get what year we're in to know what AFL season we are in
        this_year = datetime.date.today().year
        #extract game data
        logger.info(f"Extracting games data for season {this_year}.")
        games_data = extract_games(squiggle_api_client=squiggle_client, year=this_year)
        #transform game data
        logger.info(f"Transforming games data.")
        games_df = transform_games(games_data)

        #get what round we're in so that we only get games from the current round
        round = current_round(games_data)
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
        logger.info(f"Loading games data to {SILVER_DATABASE_NAME}.")
        games_loader = PostgresSqlLoader(silver_postgres_client)
        games_loader.load_games(df=games_df, load_method=pipeline_config.get("games_load_method"))

        #load odds data to postgres db
        logger.info(f"Loading odds data to {SILVER_DATABASE_NAME}.")
        odds_loader = PostgresSqlLoader(silver_postgres_client)
        odds_loader.load_odds(df=odds_df, load_method=pipeline_config.get("odds_load_method"))

        #load team fantasy data to postgres db
        logger.info(f"Loading team fantasy data to {SILVER_DATABASE_NAME}.")
        team_fantasy_loader = PostgresSqlLoader(silver_postgres_client)
        team_fantasy_loader.load_team_fantasy(df=team_fantasy_df, load_method=pipeline_config.get("tf_load_method"))

        #sql query silver layer db db and return as df
        logger.info(f"Getting SQL files from sql/extract.")
        environment = Environment(loader=FileSystemLoader(pipeline_config.get("sql_extract_path")))
        logger.info(f"Extracting summary query data from {SILVER_DATABASE_NAME}.")
        summary_template = environment.get_template("summary.sql")
        summary_data = extract_postgres(postgres_client=silver_postgres_client, sql_template=summary_template)
        if not summary_data:
            # issue warning SQL query result is empty and don't update.
            logger.warning(f"Query result from {SILVER_DATABASE_NAME} is empty.")
        else:
            #load summary data to gold layer
            summary_df = pd.json_normalize(summary_data)
            logger.info(f"Loading summary data to {GOLD_DATABASE_NAME}.")
            summary_loader = PostgresSqlLoader(gold_postgres_client)
            summary_loader.load_summary(df=summary_df, load_method=pipeline_config.get("summary_load_method"))

        metadata_logging.log(
        status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )
        print(f"Pipeline succeeded.")
    except Exception as e:
        print(f"Pipeline failed with exception {e}.")
        pipeline_logging.logger.opt(exception=e).error(f"Pipeline failed with exception {e}")
        metadata_logging.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )