from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
import json

spark = SparkSession.builder.appName("IndexUniversityNews").getOrCreate()

df = spark.read.format("hudi").load(
    "/opt/spark/work-dir/data/lakehouse/hudi/university_news"
)

es = Elasticsearch("http://elasticsearch:9200")

for row in df.toJSON().collect():
    doc = json.loads(row)

    es.index(
        index="university_news",
        id=doc["record_id"],
        document=doc
    )

print("University News indexed successfully!")

spark.stop()