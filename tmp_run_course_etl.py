import sys
sys.path.insert(0, "/opt/spark/work-dir")
from src.transformations.config.spark_config import SparkConfig
from src.transformations.etl.course_catalog_etl import run_course_catalog_etl

spark = SparkConfig(master="spark://spark-master:7077").build()
count = run_course_catalog_etl(spark)
spark.stop()
print(f"course_catalog ETL completed: {count} records")
sys.exit(0 if count > 0 else 1)
