"""
Transformation USMS Faculty -> schéma curated `faculty_profiles`.

Source réelle (usms_scraper.py -> save_consolidated_data()) :
  bucket   : raw-json
  path     : faculty_profiles/year=YYYY/month=MM/day=DD/faculty_profiles_*.json
  contenu  : UN objet JSON par fichier, clé "faculty_members" (liste imbriquée).
             Chaque entrée a déjà : record_id, source_system, source_url,
             content_hash, crawl_timestamp, business_timestamp, is_deleted,
             language, normalized_text (vide) + name, title, email,
             department, faculty, university, city, country.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transformations.spark.clean_data import normalize_text, drop_null_and_duplicates


def transform_faculty(raw_df: DataFrame) -> DataFrame:
    exploded = raw_df.select(F.explode("faculty_members").alias("m"))

    df = exploded.select(
        F.col("m.record_id").alias("record_id"),
        F.col("m.source_system").alias("source_system"),
        F.col("m.source_url").alias("source_url"),
        F.col("m.content_hash").alias("content_hash"),
        F.col("m.crawl_timestamp").cast("timestamp").alias("crawl_timestamp"),
        F.col("m.business_timestamp").cast("timestamp").alias("business_timestamp"),
        F.col("m.is_deleted").alias("is_deleted"),
        F.col("m.language").alias("language"),
        F.col("m.name").alias("full_name"),
        F.col("m.title").alias("position"),
        F.col("m.email").alias("email"),
        F.col("m.department").alias("department"),
        F.col("m.faculty").alias("faculty"),
        F.col("m.university").alias("university"),
        F.col("m.city").alias("city"),
        F.col("m.country").alias("country"),
    )

    df = normalize_text(df, source_col="full_name")
    df = df.na.fill({"faculty": "unknown", "department": "unknown", "email": ""})
    df = drop_null_and_duplicates(df, not_null_col="full_name")

    return df