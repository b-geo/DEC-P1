import yaml
import os
from dotenv import load_dotenv
from pathlib import Path
from importlib import import_module
from main.connectors.postgresql import PostgreSqlClient

if __name__ == "__main__":
    load_dotenv()
    #Create PosrgreSQL client for Logging DB
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DB_USERNAME = os.environ.get("LOGGING_DB_USERNAME")
    LOGGING_DB_PASSWORD = os.environ.get("LOGGING_DB_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_DB_USERNAME,
        password=LOGGING_DB_PASSWORD,
        port=LOGGING_PORT
    )

    #Get config for each pipeline
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            pipeline_list = [pipeline for pipeline in pipeline_config.get("pipelines")]
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )
    #Run each pipeline
    for pipeline in pipeline_list:
        pipeline_name = pipeline.get("name")
        pipeline_config = pipeline.get("config")
        module = import_module(name=f".{pipeline_name}", package="main.pipelines")
        module.run(pipeline_name=pipeline_name, pipeline_config=pipeline_config, postgres_logging_client=postgresql_logging_client)