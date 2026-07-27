from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
import json

spark = SparkSession.builder.appName("IndexFaculty").getOrCreate()

print("Reading Hudi...")

df = spark.read.format("hudi").load(
    "/opt/spark/work-dir/data/lakehouse/hudi/faculty_profiles"
)

print("Rows:", df.count())

es = Elasticsearch("http://university-elasticsearch:9200")

for row in df.toJSON().collect():
    doc = json.loads(row)

    es.index(
        index="faculty_profiles",
        id=doc["record_id"],
        document=doc
    )

print("Done")

spark.stop()