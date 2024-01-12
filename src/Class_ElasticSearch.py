from elasticsearch import Elasticsearch

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
