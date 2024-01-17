from elasticsearch import Elasticsearch
import psycopg2
import pandas as pd
from io import StringIO
import re

class Elasticsearch_conn:

    def __init__(self, elastic_password, elastic_username, elastic_index_name):
        self.elastic_password = elastic_password
        self.elastic_username = elastic_username
        self.elastic_path = "http://localhost:9200"
        self.elastic_index_name = elastic_index_name
        self.client = Elasticsearch(
            self.elastic_path,
            verify_certs=False,
            basic_auth=(self.elastic_username, self.elastic_password)
        )


    def load_data (self, list_doc):
        for id_for_elastic, document in enumerate(list_doc):
            self.client.index(
                index=self.elastic_index_name,
                body=document,
                id=id_for_elastic,
                )


class postgresql_conn:

    def __init__(self, params_dict):
        self.params_dict = params_dict

    def connect(self):
        self.conn = psycopg2.connect(**self.params_dict)
        self.conn.autocommit = True

    def load_df(self, df, table_name):
        """
        Load Pandas DataFrame into PostgreSQL
        """

        # Clean table name
        table_name = re.sub(r'[^a-zA-Z0-9_]', '', table_name)

        # Clean column names
        df.columns = df.columns.str.lower().str.replace(' ', '_')

        # Map pandas data types to PostgreSQL data types
        pg_data_types = {
            'int64': 'integer',
            'float64': 'float',
            'object': 'text',
            'bool': 'boolean',
            'datetime64[ns]': 'timestamp'
        }

        # Convert pandas data types to PostgreSQL data types
        df_types = df.dtypes.apply(lambda x: pg_data_types.get(str(x), 'text'))
        data_types = [f"{col} {dtype}" for col, dtype in zip(df.columns, df_types)]

        # Create table query
        columns = ",".join(data_types)
        create_query = f"""CREATE TABLE IF NOT EXISTS {table_name} ({columns})"""

        # Replace NaN values with NULL in the DataFrame
        df = df.where(pd.notna(df), None)

        # Load data
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep='\\N')  # Use '\\N' for NULL representation
        buffer.seek(0)

        cur = self.conn.cursor()
        cur.execute(create_query)
        cur.copy_from(buffer, table_name, sep=",", null="\\N")  # Specify NULL representation for copy_from


if __name__ == "__main__":
    # Connect
    params = {
        "host": "localhost",
        "dbname": "postgres",
        "user": "postgres",
        "password": "1234"
    }
    db = postgresql_conn(params)
    db.connect()
    # df_try = pd.DataFrame({
    #     'Column1': [1, 2, 3],
    #     'Column2': ['a', 'b', 'c']
    # })
    # db.load_df(df_try, 'try')
    name = '../Data_files/csv files/information_clubs.csv'
    df_club = pd.read_csv(name).head()
    df_club = df_club.iloc[:, 1:]
    db.load_df(df_club,'club')