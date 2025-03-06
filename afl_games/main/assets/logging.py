import sys
from datetime import datetime
from loguru import logger
from datetime import datetime
from sqlalchemy import Table, Column, Integer, String, MetaData, JSON
from sqlalchemy import insert, select, func
from main.connectors.postgresql import PostgreSqlClient

class PipelineLogging:
    """A logging class for logging to a .log file."""
    def __init__(
        self,
        pipeline_name: str,
        log_folder_path: str,
        ):
        self.file_path = f"{log_folder_path}\\{datetime.now().strftime('%Y-%m-%d')}_{pipeline_name}.log"
        logger.remove(0)
        logger.add(self.file_path, format="{time} | {level} | {message}")
        logger.add(sys.stderr, format="{time} | {level} | {message}")
        self.logger = logger
        self.logger_start = datetime.now()
    def get_logs(self) -> str:
        try:
            with open(self.file_path, "r") as file:
                return "".join([line for line in file.readlines()])
        except BaseException as e:
            raise Exception("Failed to get log file.")

class MetaDataLoggingStatus:
    """A data class specifying statuses of the pipeline."""

    RUN_START = "start"
    RUN_SUCCESS = "success"
    RUN_FAILURE = "fail"

class MetaDataLogging:
    """A metadata logging class for logging the overall status of the pipeline and the logs of the pipeline."""
    def __init__(
        self,
        pipeline_name: str,
        postgresql_client: PostgreSqlClient,
        config: dict = {},
        log_table_name: str = "pipeline_logs",
    ):
        self.pipeline_name = pipeline_name
        self.log_table_name = log_table_name
        self.postgresql_client = postgresql_client
        self.config = config
        self.metadata = MetaData()
        self.table = Table(
            self.log_table_name,
            self.metadata,
            Column("pipeline_name", String, primary_key=True),
            Column("run_id", Integer, primary_key=True),
            Column("timestamp", String),
            Column("status", String),
            Column("config", JSON),
            Column("logs", String),
        )
        self.run_id: int = self._get_run_id()

    def _create_log_table(self) -> None:
        """Create log table if it does not exist."""
        self.postgresql_client.create_table(table_name= self.log_table_name, metadata=self.metadata)

    def _get_run_id(self) -> int:
        """Gets the next run id. Sets run id to 1 if no run id exists."""
        self._create_log_table()
        run_id = self.postgresql_client.engine.execute(
            select(func.max(self.table.c.run_id)).where(
                self.table.c.pipeline_name == self.pipeline_name
            )
        ).first()[0]
        if run_id is None:
            return 1
        else:
            return run_id + 1

    def log(
        self,
        status: MetaDataLoggingStatus = MetaDataLoggingStatus.RUN_START,
        timestamp: datetime = None,
        logs: str = None,
    ) -> None:
        """Writes pipeline metadata log to a database"""
        if timestamp is None:
            timestamp = datetime.now()
        insert_statement = insert(self.table).values(
            pipeline_name=self.pipeline_name,
            timestamp=timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            run_id=self.run_id,
            status=status,
            config=self.config,
            logs=logs,
        )
        self.postgresql_client.engine.execute(insert_statement)