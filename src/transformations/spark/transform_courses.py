"""
Transformation MIT OCW (documents PDF) -> schéma curated `course_catalog`.

⚠️ Le scraper (mit_ocw_pdf_scraper.py) moissonne des documents génériques
(PDF/DOC/XLS/...), pas des cours structurés. On reconstruit une
approximation à partir de ce qui existe :
  - department   <- déduit de l'URL (/courses/<departement>/...)
  - course_title <- nom de fichier (sans extension)

Source réelle :
  bucket   : raw-json
  path     : source=mit_ocw/year=YYYY/month=MM/day=DD/*_metadata.json
  contenu  : UN objet JSON par fichier : record_id, source_system,
             source_url, content_hash, crawl_timestamp, file_name,
             file_size_bytes, content_type, raw_storage_path (pointe vers
             le PDF binaire dans raw-docs).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transformations.spark.clean_data import normalize_text, drop_null_and_duplicates


def transform_courses(raw_df: DataFrame) -> DataFrame:
    df = raw_df.select(
        F.col("record_id").alias("record_id"),
        F.col("source_system").alias("source_system"),
        F.col("source_url").alias("source_url"),
        F.col("content_hash").alias("content_hash"),
        F.col("crawl_timestamp").cast("timestamp").alias("crawl_timestamp"),
        F.col("file_name").alias("file_name"),
        F.col("file_size_bytes").alias("file_size_bytes"),
        F.col("content_type").alias("content_type"),
        F.col("raw_storage_path").alias("raw_storage_path"),
    )

    df = df.withColumn(
        "department",
        F.coalesce(F.regexp_extract(F.col("source_url"), r"/courses/([^/]+)/", 1), F.lit("unknown")),
    )
    df = df.withColumn("course_title", F.regexp_replace(F.col("file_name"), r"\.[a-zA-Z0-9]+$", ""))
    df = df.withColumn("business_timestamp", F.col("crawl_timestamp"))
    df = df.withColumn("is_deleted", F.lit(False))
    df = df.withColumn("language", F.lit("en"))
    df = normalize_text(df, source_col="course_title")

    df = df.na.fill({"department": "unknown"})
    df = drop_null_and_duplicates(df, not_null_col="record_id")

    return df