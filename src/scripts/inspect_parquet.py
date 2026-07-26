from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("inspect").getOrCreate()
df = spark.read.parquet("s3a://hudi-curated/faculty_profiles/source_system=openalex")
df.printSchema()
print("Count:", df.count())
df.show(3, truncate=False)
spark.stop()
