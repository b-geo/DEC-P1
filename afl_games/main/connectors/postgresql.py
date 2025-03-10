from sqlalchemy import create_engine, Table, MetaData, Column, inspect
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql

class PostgreSqlClient:
    """A client to to perform select queries on a PostgreSQL database."""
    def __init__(
        self,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int = 5432
    ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )

        self.engine = create_engine(connection_url)

    def select_all(self, table: Table) -> list[dict]:
        """Get all tables in databased and return the result in a list of dictionaries."""
        return [dict(row) for row in self.engine.execute(table.select()).all()]

    def run_sql(self, sql: str) -> list[dict]:
        """
        Execute SQL code provided and returns the result in a list of dictionaries.
        This method should only be used if you expect a resultset to be returned.
        """
        return [dict(row) for row in self.engine.execute(sql).all()]

    def get_metadata(self) -> MetaData:
        """Get the metadata object for all tables for a given database"""
        metadata = MetaData(bind=self.engine)
        metadata.reflect()
        return metadata

    def get_table_schema(self, table_name: str) -> Table:
        """Get the table schema and metadata"""
        metadata = self.get_metadata()
        return metadata.tables[table_name]

    def table_exists(self, table_name: str) -> bool:
        """Check if the table already exists in the database."""
        return inspect(self.engine).has_table(table_name)

    def create_table(self, table_name: str, metadata: MetaData) -> None:
        """Create a single table provided in the metadata object"""
        existing_table = metadata.tables[table_name]
        new_metadata = MetaData()
        columns = [
            Column(column.name, column.type, primary_key=column.primary_key)
            for column in existing_table.columns
        ]
        new_table = Table(table_name, new_metadata, *columns)
        new_metadata.create_all(bind=self.engine)

    def create_all_tables(self, metadata: MetaData) -> None:
        """Create tables provided in the metadata object"""
        metadata.create_all(self.engine)

    def drop_table(self, table_name: str) -> None:
        """Drop a specified table if it exists"""
        self.engine.execute(f"drop table if exists {table_name};")

    def insert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """Insert data into a database table. If the table doesn't exist, it is created."""
        self.create_table(table_name=table.name, metadata=metadata)
        insert_statement = postgresql.insert(table).values(data)
        self.engine.execute(insert_statement)

    def overwrite(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """Overwrite data into a database table. If the table doesn't exist, it is created."""
        self.drop_table(table.name)
        self.insert(data=data, table=table, metadata=metadata)

    def upsert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """Upsert data into a database table. If the table doesn't exist, it is created."""
        self.create_table(table_name=table.name, metadata=metadata)
        key_columns = [
            pk_column.name for pk_column in table.primary_key.columns.values()
        ]
        insert_statement = postgresql.insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in key_columns
            },
        )
        self.engine.execute(upsert_statement)