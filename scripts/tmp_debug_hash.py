import sys
sys.path.insert(0, "/opt/spark/work-dir")
from pyspark.sql import SparkSession, functions as F
from src.transformations.readers.minio_reader import discover_source_prefixes, read_json
from src.transformations.etl.course_catalog_etl import COURSE_ARRAY_FIELDS
from src.transformations.transformers.course_transformer import transform_course_catalog

spark = SparkSession.builder \
    .appName("DebugHash") \
    .master("spark://spark-master:7077") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

raw = read_json(spark, "raw-json", prefix="source=openalex/")
exploded = raw.selectExpr("inline_outer(results)", "input_file_name() as _source_file")
print(f"Exploded count: {exploded.count()}")
print(f"Columns: {exploded.columns[:20]}")
print()

# Preview record_id generation
row_hash = F.sha2(F.to_json(F.struct(F.col("*"))), 256)
test_df = exploded.select(
    F.col("id"),
    F.col("display_name").alias("course_name"),
    F.lit("openalex").alias("source_system"),
    F.col("id").alias("record_id_raw")
).limit(20)
test_df.show(truncate=False)

# Check for columns that might cause duplicate hashes
# The issue might be that many records share the same "display_name" or "id" is not included
print("Top 20 columns:")
for c in exploded.columns[:20]:
    print(f"  {c}")

spark.stop()
