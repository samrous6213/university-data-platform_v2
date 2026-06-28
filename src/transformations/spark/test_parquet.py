from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("spark://spark-master:7077")
    .appName("Test Parquet")
    .getOrCreate()
)

df = spark.read.parquet(
    "/opt/spark/work-dir/data/processed_html"
)

print(f"Rows = {df.count()}")

df.select(
    "source_system",
    "page_title"
).show(20, truncate=False)

spark.stop()