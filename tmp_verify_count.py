import sys
sys.path.insert(0, "/opt/spark/work-dir")
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyCount") \
    .master("spark://spark-master:7077") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

df = spark.read.format("hudi").load("s3a://hudi-curated/course_catalog")
count = df.count()
print(f"=== DIRECT HUDI READ: count={count} ===")
df.select("record_id", "course_code", "faculty").show(10, truncate=False)
print(f"=== HUDI READ COMPLETE ===")

spark.stop()
