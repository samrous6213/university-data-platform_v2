import sys
sys.path.insert(0, "/opt/spark/work-dir")
from pyspark.sql import SparkSession, functions as F
from src.transformations.readers.minio_reader import discover_source_prefixes, read_json
from src.transformations.etl.course_catalog_etl import COURSE_ARRAY_FIELDS
from src.transformations.transformers.course_transformer import transform_course_catalog

spark = SparkSession.builder \
    .appName("DebugHash2") \
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
print(f"Top 50 columns: {exploded.columns[:50]}")
print()

# Check unique IDs
unique_ids = exploded.select("id").distinct().count()
print(f"Unique 'id' values: {unique_ids}")
unique_display = exploded.select("display_name").distinct().count()
print(f"Unique 'display_name' values: {unique_display}")

# Check the record_id generation
# simulate what transformer does  
tf = transform_course_catalog(exploded)
print(f"\nAfter transform: {tf.count()} rows")
print(f"Transform columns: {tf.columns}")
unique_rid = tf.select("record_id").distinct().count()
print(f"Distinct record_ids: {unique_rid}")

# Show some record_ids
print("\nSample record_ids:")
tf.select("record_id", "source_system", "course_name", "course_code", "faculty", "department").show(10, truncate=False)

# Check what content_hash looks like
print("\nSample content_hashes:")
tf.select("content_hash", "record_id").show(10, truncate=False)

spark.stop()
