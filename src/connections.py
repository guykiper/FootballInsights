from elasticsearch import Elasticsearch
from elasticsearch import helpers
import elasticsearch
import psycopg2
import pandas as pd
from io import StringIO
import re
import subprocess
import os


class Elasticsearch_conn:

    def __init__(self, elastic_password='changeme', elastic_username='elastic'):
        self.elastic_password = elastic_password
        self.elastic_username = elastic_username
        self.elastic_path = "http://localhost:9200"
        self.client = Elasticsearch(
            self.elastic_path,
            verify_certs=False,
            basic_auth=(self.elastic_username, self.elastic_password)
        )

    def check_connection(self):
        try:
            # Ping the Elasticsearch cluster
            if self.client.ping():
                return "Connected to Elasticsearch cluster."
            else:
                return "Failed to connect to Elasticsearch cluster."
        except Exception as e:
            return f"Error checking Elasticsearch connection: {e}"

    def start_elasticsearch(self, path_file='../Data_files/connections_info/elastic_and_kibana_connection.txt'):
        """Start Elasticsearch and Kibana"""

        # with open(path_file) as f:
        #
        #     lines = f.readlines()
        #     es_path = lines[0].strip()
        #     os.system(f'"{es_path}"')
        es_path = r'C:\Users\moodi\Desktop\guy\General Studies\udemy\Elastic Search\elasticsearch-8.7.1-windows-x86_64\elasticsearch-8.7.1\bin\elasticsearch.bat'
        os.system(f'"{es_path}"')

    # C:\Users\moodi\Desktop\guy\General Studies\udemy\Elastic Search\elasticsearch-8.7.1-windows-x86_64\elasticsearch-8.7.1\bin\elasticsearch.bat

    def get_indices(self):
        try:
            indices_data = self.client.cat.indices(format='json')
            indices_list = [index_data['index'] for index_data in indices_data]
            return indices_list
        except Exception as e:
            return f"Error getting indices: {e}"

    def generate_mapping(self, dataframe):
        mapping = {"properties": {}}

        for column, dtype in dataframe.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                mapping["properties"][column] = {"type": "integer"}
            elif pd.api.types.is_float_dtype(dtype):
                mapping["properties"][column] = {"type": "float"}
            elif pd.api.types.is_bool_dtype(dtype):
                mapping["properties"][column] = {"type": "boolean"}
            elif pd.api.types.is_string_dtype(dtype):
                mapping["properties"][column] = {"type": "keyword"}
            else:
                # You can handle other data types as needed
                pass

        return mapping

    def load_data(self, dataframe, elastic_index_name):

        # Handle NaN values
        dataframe = dataframe.fillna(0)  # Replace NaN with 0, adjust as needed

        # Generate mapping based on DataFrame
        mapping = self.generate_mapping(dataframe)

        # Create index with the generated mapping
        self.client.indices.create(index=elastic_index_name, body={"mappings": mapping})

        # Convert dataframe to docs
        docs = dataframe.to_dict(orient="records")

        # Index docs
        for id_for_elastic, doc in enumerate(docs):
            self.client.index(
                index=elastic_index_name,
                body=doc,
                id=id_for_elastic
            )

    def query_data(self, elastic_index_name, query):
        """Execute a query on the indexed data"""
        results = self.client.search(
            index=elastic_index_name,
            body={
                "query": query
            }
        )

        hits = results['hits']['hits']
        return [hit["_source"] for hit in hits]


class postgresql_conn:

    def __init__(self, params_dict):
        self.params_dict = params_dict
        self.conn = psycopg2.connect(**self.params_dict)

    def check_connection(self):
        try:
            # Get the status of the connection
            conn_status = self.conn.status

            # Check if the connection is open
            if conn_status == psycopg2.extensions.STATUS_READY:
                return "Connected to PostgreSQL database."
            else:
                return "Failed to connect to PostgreSQL database."
        except (Exception, psycopg2.Error) as error:
            return f"Error checking PostgreSQL connection: {error}"
        finally:
            # Close the connection if it was opened
            if self.conn:
                self.conn.close()

    def connect(self):
        self.conn = psycopg2.connect(**self.params_dict)
        self.conn.autocommit = True

    def execute_query(self, list_query_text):
        cursor = self.conn.cursor()
        results = []
        for query_text in list_query_text:
            cursor.execute(query_text)
            results.append(pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description]))
        self.conn.commit()
        self.conn.close()
        return results

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
    res = db.execute_query(["select id, player from players_info"])
    print(res)
    # db.connect()
    # list_query = db.execute_query(['SELECT * FROM public.players_info;'])
    # print('finish')

    # es = Elasticsearch_conn()
    # es.start_elasticsearch()
    # es.check_connection()
    # query = {"match_all":{}}
    # res = es.get_indices()
    # print(res)
    # res = es.query_data('players_per_90_minutes',query)
    # print('finish')
    # es.start_es_kibana(path_file="../Data_files/connections_info/elastic_and_kibana_connection.txt")
    # Define the Elasticsearch command
    # Run the command

    # path_file = '../Data_files/connections_info/elastic_and_kibana_connection.txt'
    #
    # with open(path_file) as f:
    #     lines = [line.strip() for line in f.readlines()]
    #
    # for line in lines:
    #     try:
    #         os.system(line)
    #     except Exception as e:
    #         try:
    #             os.startfile(line)
    #         except Exception as e:
    #             print(f"Error executing or opening '{line}': {e}")
    # elasticsearch_command = r'C:\Users\moodi\Desktop\guy\General Studies\udemy\Elastic Search\elasticsearch-8.7.1-windows-x86_64\elasticsearch-8.7.1\bin\elasticsearch.bat'
    # #
    # # # Run the command
    # subprocess.run(elasticsearch_command, shell=True)
