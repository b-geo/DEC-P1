import pandas as pd
from main.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float

class PostgresSqlLoader:
    def __init__(
            self,
            postgresql_client: PostgreSqlClient
            ):
        self.client = postgresql_client
        self.metadata = MetaData()
    def _load(
        self,
        df: pd.DataFrame,
        table: Table,
        load_method: str = "overwrite",
    ) -> None:
        """
        Load dataframe to a database.

        Args:
            df: dataframe to load
            self.client: postgresql client
            table: sqlalchemy table
            metadata: sqlalchemy metadata
            load_method: supports one of: [insert, upsert, overwrite]
        
        Raises:
        """
        if load_method == "insert":
            self.client.insert(
                data=df.to_dict(orient="records"), table=table, metadata=self.metadata
            )
        elif load_method == "upsert":
            self.client.upsert(
                data=df.to_dict(orient="records"), table=table, metadata=self.metadata
            )
        elif load_method == "overwrite":
            self.client.overwrite(
                data=df.to_dict(orient="records"), table=table, metadata=self.metadata
            )
        else:
            raise Exception(
                "Please specify a correct load method: [insert, upsert, overwrite]"
            )
    def load_games(self, df: pd.DataFrame, load_method: str = "upsert") -> None:
        games_table = Table(
            "games",
            self.metadata,
            Column("game_id", Integer, primary_key=True),
            Column("game_data_updated", String),
            Column("game_time", String),
            Column("home_team", String),
            Column("away_team", String),
            Column("round", Integer),
            Column("complete_perc", Integer)
        )
        self._load(
            df=df,
            table=games_table,
            load_method=load_method
        )
    def load_odds(self, df: pd.DataFrame, load_method: str = "overwrite") -> None:
        odds_table = Table(
            "odds",
            self.metadata,
            Column("game_id", Integer, primary_key=True),
            Column("odds_data_updated", String),
            Column("tip", String),
            Column("projected_margin", Float),
            Column("confidence", Float)
        )
        self._load(
            df=df,
            table=odds_table,
            load_method=load_method
        )
    def load_team_fantasy(self, df: pd.DataFrame, load_method: str = "overwrite") -> None:
        team_fantasy_table = Table(
            "team_fantasy",
            self.metadata,
            Column("team", String, primary_key=True),
            Column("team_fantasy_score", Integer)
        )
        self._load(
            df=df,
            table=team_fantasy_table,
            load_method=load_method
        )
    def load_summary(self, df: pd.DataFrame, load_method: str = "overwrite") -> None:
        summary_table = Table(
            "summary",
            self.metadata,
            Column("game_id", Integer, primary_key=True),
            Column("game_time", String),
            Column("round", Integer),
            Column("home_team", String),
            Column("home_team_fantasy", Integer),
            Column("away_team", String),
            Column("away_team_fantasy", Integer),
            Column("tip", String),
            Column("projected_margin", Float),
            Column("confidence", Float)
        )
        self._load(
            df=df,
            table=summary_table,
            load_method=load_method
        )

