import sys
sys.path.insert(0, "/opt/spark/work-dir")
from pyspark.sql import SparkSession, functions as F
from src.transformations.readers.minio_reader import discover_source_prefixes, read_json
from src.transformations.etl.course_catalog_etl import COURSE_ARRAY_FIELDS
from src.transformations.transformers.course_transformer import transform_course_catalog
from src.transformations.readers.minio_reader import extract_source_name

spark = SparkSession.builder \
    .appName("DebugSource") \
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

all_prefixes = discover_source_prefixes(spark, "raw-json")
print(f"Source prefixes: {all_prefixes}")

for prefix in all_prefixes:
    raw = read_json(spark, "raw-json", prefix=prefix)
    print(f"\nPrefix: {prefix}")
    print(f"Raw columns: {raw.columns}")
    matched = [f for f in COURSE_ARRAY_FIELDS if f in raw.columns]
    print(f"Matched array field: {matched}")
    if matched:
        exploded = raw.selectExpr(f"inline_outer({matched[0]})", "input_file_name() as _source_file")
        exploded_count = exploded.count()
        print(f"  After explode: {exploded_count} rows")
        print(f"  Columns: {exploded.columns[:15]}")
        exploded.select("id", "display_name", "source_system").show(3, truncate=False)
        tf = transform_course_catalog(exploded)
        tf_count = tf.count()
        print(f"  After transform: {tf_count} rows")
        distinct_record_ids = tf.select("record_id").distinct().count()
        print(f"  Distinct record_ids: {distinct_record_ids}")

spark.stop()
