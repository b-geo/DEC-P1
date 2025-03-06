import pandas as pd
from sqlalchemy import Table
from datetime import datetime

def compare_table_schema(source_table: Table, target_table: Table) -> None:
    """Checks two database tables and raises exception if they don't match.

    Args:
        source_table (Table): A SQLAlchemy Table class
        target_table (Table): A SQLAlchemy Table class

    Raises:
        Exception: Table <target table name> in target DB does not contain column <column in source table>.
        Exception: Source column '<source column name>' is <source column type> but expected <target column type>.
    """
    for source_column in source_table.c:
        if source_column.name not in target_table.c:
            raise Exception(f"Table {target_table.name} in target DB does not contain column {source_column.name}.")
        target_column = target_table.c[source_column.name]
        if not isinstance(target_column.type, source_column.type.__class__):
            print(f"Source column '{source_column.name}' is {source_column.type} but expected {target_column.type}.")
            raise Exception(f"Source column '{source_column.name}' is {source_column.type} but expected {target_column.type}.")

def current_round(df: pd.DataFrame) -> int:
    """Identifies current AFL round from games data.

    Args:
        df (pd.DataFrame): Games as rows, specifying game date, completion and round.

    Returns:
        int: The AFL round.
    """
    df = df.sort_values(by=["unixtime"], ascending=True)
    df["localtime"] = df.apply(lambda x: datetime.strptime(x["localtime"], "%Y-%m-%d %H:%M:%S%z"), axis=1)
    df["now_in_tz"] = df.apply(lambda x: datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"+x["tz"]), "%Y-%m-%d %H:%M:%S%z"), axis=1)
    df_filtered = df.loc[
        (df["complete"] == 0) &
        (df["localtime"] > df["now_in_tz"])
        ]
    try:
        round = df_filtered["round"].iloc[0]
    except:
        print("There are no rounds with incomplete games in the future. Defaulting to most recent completed round.")
        df_except = df.loc[df["localtime"] < df["now_in_tz"]]
        round = df_except["round"].iloc[-1]
    return round