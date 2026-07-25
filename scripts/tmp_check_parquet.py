import sys
sys.path.insert(0, "/opt/spark/work-dir")
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckParquet") \
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

# Read raw parquet files directly
raw = spark.read.parquet("s3a://hudi-curated/course_catalog/*.parquet")
print(f"=== RAW PARQUET: count={raw.count()} ===")
raw.select("record_id", "faculty", "source_system").show(5, truncate=False)

# Also read through Hudi
hudi = spark.read.format("hudi").load("s3a://hudi-curated/course_catalog")
print(f"=== HUDI READ: count={hudi.count()} ===")
hudi.select("record_id", "faculty", "source_system").show(5, truncate=False)

spark.stop()
