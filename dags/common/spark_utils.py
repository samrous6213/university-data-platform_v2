from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging

def get_spark_session(app_name="UniversityDataPlatform"):
    """Create and return a Spark session with Hudi support"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

def get_faculty_profile_schema():
    """Standard schema for faculty_profiles table"""
    return StructType([
        StructField("record_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("title", StringType(), True),
        StructField("department", StringType(), True),
        StructField("email", StringType(), True),
        StructField("research_interests", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("crawl_timestamp", TimestampType(), True),
        StructField("year", IntegerType(), True)
    ])

def get_course_catalog_schema():
    """Standard schema for course_catalog table"""
    return StructType([
        StructField("record_id", StringType(), True),
        StructField("course_id", StringType(), True),
        StructField("course_name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("credits", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("instructor", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("crawl_timestamp", TimestampType(), True),
        StructField("year", IntegerType(), True)
    ])
