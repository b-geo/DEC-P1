import pandas as pd
from sqlalchemy import Table

#use as test table
# from load: compare_table_schema(table, postgresql_client.get_table_schema(table_name=table.name))
def compare_table_schema(source_table: Table, target_table: Table) -> None:
    """checks that the tables declared in metadata are actually in the db"""
    for source_column in source_table.c:
        if source_column.name not in target_table.c:
            raise Exception(f"Table {target_table.name} in target DB does not contain column {source_column.name}")
        target_column = target_table.c[source_column.name]
        if not isinstance(target_column.type, source_column.type.__class__):
            raise Exception(f"Source column '{source_column.name}' is {source_column.type} but expected {target_column.type}.")
    #should i return false if not the same then check for that?

def current_round(df: pd.DataFrame) -> int:
    """takes in df of games sorted by unix time and gets first one not complete"""
    df_filtered = df.loc[df["complete_perc"] == 0]
    current_round = df_filtered["round"].iloc[0]
    return current_round