from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
import json

spark = SparkSession.builder.appName("IndexDocumentsRegistry").getOrCreate()

df = spark.read.format("hudi").load(
    "/opt/spark/work-dir/data/lakehouse/hudi/documents_registry"
)

es = Elasticsearch("http://elasticsearch:9200")

for row in df.toJSON().collect():
    doc = json.loads(row)

    es.index(
        index="documents_registry",
        id=doc["record_id"],
        document=doc
    )

print("Documents Registry indexed successfully!")

spark.stop()