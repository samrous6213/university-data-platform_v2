import sys, traceback
sys.path.insert(0, "/opt/spark/work-dir")
from pyspark.sql import SparkSession, functions as F
from src.transformations.readers.minio_reader import discover_source_prefixes, read_json
from src.transformations.etl.course_catalog_etl import COURSE_ARRAY_FIELDS
from src.transformations.transformers.course_transformer import transform_course_catalog

spark = SparkSession.builder \
    .appName("DebugSource2") \
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
    try:
        raw = read_json(spark, "raw-json", prefix=prefix)
        print(f"\n=== Prefix: {prefix} ===")
        print(f"Raw columns: {raw.columns}")
        matched = [f for f in COURSE_ARRAY_FIELDS if f in raw.columns]
        print(f"Matched: {matched}")
        if not matched:
            print("Skipping - no matching array field")
            continue
        exploded = raw.selectExpr(f"inline_outer({matched[0]})", "input_file_name() as _source_file")
        before = exploded.count()
        print(f"After explode: {before} rows")
        print(f"Columns: {exploded.columns[:10]}")
        
        # Check for record_id and content_hash in exploded
        has_rid = "record_id" in exploded.columns
        has_ch = "content_hash" in exploded.columns
        print(f"Has record_id: {has_rid}, Has content_hash: {has_ch}")
        if has_rid:
            distinct_rid = exploded.select("record_id").distinct().count()
            print(f"Distinct record_ids in source: {distinct_rid}")
        
        tf = transform_course_catalog(exploded)
        after = tf.count()
        print(f"After transform: {after} rows")
        distinct_tf = tf.select("record_id").distinct().count()
        print(f"Distinct record_ids after transform: {distinct_tf}")
        tf.select("record_id", "source_system").show(5, truncate=False)
    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()

spark.stop()
