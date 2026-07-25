from elasticsearch import Elasticsearch

es = Elasticsearch("http://127.0.0.1:9200")

print(es.info())