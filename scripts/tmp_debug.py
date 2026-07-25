import sys
sys.path.insert(0, "/opt/spark/work-dir")
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Debug") \
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

# Direct Parquet
raw = spark.read.parquet("s3a://hudi-curated/course_catalog/*.parquet")
print(f"RAW PARQUET count={raw.count()}")
print("Source system distribution:")
raw.groupBy("source_system").count().show(100, truncate=False)
print("First 5 record_ids:")
raw.select("record_id").show(5, truncate=False)

# Hudi read  
hudi = spark.read.format("hudi").load("s3a://hudi-curated/course_catalog")
print(f"HUDI count={hudi.count()}")
hudi.select("source_system", "_hoodie_commit_time").show(5, truncate=False)

spark.stop()
