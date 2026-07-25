import sys
sys.path.insert(0, "/opt/spark/work-dir")
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder \
    .appName("Debug2") \
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

raw = spark.read.parquet("s3a://hudi-curated/course_catalog/*.parquet")
print(f"RAW count={raw.count()}")
dist = raw.groupBy("source_system").agg(F.count("*").alias("cnt")).collect()
for row in dist:
    print(f"  source_system={row['source_system']}: count={row['cnt']}")
print("First 3 rows:")
raw.select("record_id", "source_system", "course_code", "course_name").show(3, truncate=False)

spark.stop()
