from pyspark.sql import SparkSession
from src.transformations.readers.minio_reader import read_json

spark = SparkSession.builder \
    .appName("check_news") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://university-minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .master("local[1]") \
    .getOrCreate()

df = read_json(spark, "raw-json", prefix="source=ensa/")
if df.count() > 0:
    st = df.schema["news_items"].simpleString()
    print(f"news_items type: {st}", flush=True)
    exploded = df.selectExpr("inline_outer(news_items)", "input_file_name() as _source_file")
    print(f"exploded columns: {exploded.columns}", flush=True)
    # check duplicates
    from collections import Counter
    cnt = Counter(exploded.columns)
    dups = [c for c, n in cnt.items() if n > 1]
    if dups:
        print(f"DUPLICATE COLUMNS: {dups}", flush=True)
    print(f"exploded count: {exploded.count()}", flush=True)
    row = exploded.limit(1).toJSON().first()
    print(f"SAMPLE: {row[:500]}", flush=True)
else:
    print("No data", flush=True)

spark.stop()
