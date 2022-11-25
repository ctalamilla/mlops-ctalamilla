import pandas as pd
from sqlalchemy import create_engine

# engine = create_engine('postgresql://airflow:airflow@postgres:5432/stocks')
class psql_Client:
    def __init__(self, db):
        self.dialect = "postgresql"
        self.db = db
        self._engine = None

    def _get_engine(self):
        db_uri = f"{self.dialect}://{self.db}"
        if not self._engine:
            self._engine = create_engine(db_uri)
        return self._engine

    def _connect(self):
        return self._get_engine().connect()

    @staticmethod
    def _cursor_columns(cursor):
        if hasattr(cursor, "keys"):
            return cursor.keys()
        else:
            return [c[0] for c in cursor.description]

    def execute(self, sql, connection=None):
        if connection is None:
            connection = self._connect()
        return connection.execute(sql)

    def insert_from_frame(self, df, table, if_exists="append", index=False, **kwargs):
        connection = self._connect()
        with connection:
            df.to_sql(table, connection, if_exists=if_exists, index=index, **kwargs)

    def to_frame(self, *args, **kwargs):
        cursor = self.execute(*args, **kwargs)
        if not cursor:
            return
        data = cursor.fetchall()
        if data:
            df = pd.DataFrame(data, columns=self._cursor_columns(cursor))
        else:
            df = pd.DataFrame()
        return df


if __name__ == "__main__":
    db = "airflow:airflow@postgres:5432/weather_data"
    # sqlite_cli = SqLiteClient(db)
    # print(sqlite_cli.to_frame('SELECT * FROM stocks_daily'))
