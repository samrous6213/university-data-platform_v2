from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch

spark = SparkSession.builder.appName("IndexResearch").getOrCreate()

df = spark.read.format("hudi").load("/opt/spark/work-dir/data/lakehouse/hudi/research_publications")

es = Elasticsearch("http://elasticsearch:9200")

for row in df.toJSON().collect():
    doc = eval(row)
    es.index(
        index="research_publications",
        id=doc["record_id"],
        document=doc
    )

print("Research publications indexed!")

spark.stop()
