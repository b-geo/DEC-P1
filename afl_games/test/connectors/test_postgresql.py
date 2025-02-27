from main.connectors.postgresql import PostgreSqlClient
import pytest
from dotenv import load_dotenv
import os
from sqlalchemy import Table, Column, Integer, String, MetaData


@pytest.fixture(scope="session")
def setup_postgresql_client():
    load_dotenv()
    #silver db
    SILVER_DATABASE_NAME = os.environ.get("SILVER_DATABASE_NAME")
    SILVER_SERVER_NAME = os.environ.get("SILVER_SERVER_NAME")
    SILVER_DB_USERNAME = os.environ.get("SILVER_DB_USERNAME")
    SILVER_DB_PASSWORD = os.environ.get("SILVER_DB_PASSWORD")
    SILVER_PORT = os.environ.get("SILVER_PORT")
    silver_postgresql_client = PostgreSqlClient(SILVER_SERVER_NAME, SILVER_DATABASE_NAME, SILVER_DB_USERNAME, SILVER_DB_PASSWORD, SILVER_PORT)
    return silver_postgresql_client


@pytest.fixture(scope="session")
def setup_table():
    table_name = "test_table"
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("value", String),
    )
    return table_name, table, metadata


def test_postgresqlclient_insert(setup_postgresql_client, setup_table):
    postgresql_client = setup_postgresql_client
    table_name, table, metadata = setup_table
    postgresql_client.drop_table(table_name)  # make sure table has already been dropped

    data = [{"id": 1, "value": "hello"}, {"id": 2, "value": "world"}]

    postgresql_client.insert(data=data, table=table, metadata=metadata)

    result = postgresql_client.select_all(table=table)
    assert len(result) == 2

    postgresql_client.drop_table(table_name)


def test_postgresqlclient_upsert(setup_postgresql_client, setup_table):
    postgresql_client = setup_postgresql_client
    table_name, table, metadata = setup_table
    postgresql_client.drop_table(table_name)  # make sure table has already been dropped

    data = [{"id": 1, "value": "hello"}, {"id": 2, "value": "world"}]

    postgresql_client.insert(data=data, table=table, metadata=metadata)

    result = postgresql_client.select_all(table=table)
    assert len(result) == 2

    postgresql_client.upsert(data=data, table=table, metadata=metadata)

    result = postgresql_client.select_all(table=table)
    assert len(result) == 2

    postgresql_client.drop_table(table_name)


def test_postgresqlclient_overwrite(setup_postgresql_client, setup_table):
    postgresql_client = setup_postgresql_client
    table_name, table, metadata = setup_table
    postgresql_client.drop_table(table_name)  # make sure table has already been dropped

    data = [{"id": 1, "value": "hello"}, {"id": 2, "value": "world"}]

    postgresql_client.insert(data=data, table=table, metadata=metadata)

    result = postgresql_client.select_all(table=table)
    assert len(result) == 2

    postgresql_client.overwrite(data=data, table=table, metadata=metadata)

    result = postgresql_client.select_all(table=table)
    assert len(result) == 2

    postgresql_client.drop_table(table_name)