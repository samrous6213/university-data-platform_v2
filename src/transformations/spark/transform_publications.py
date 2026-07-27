"""
Transformation Crossref (publications) -> schéma curated `research_publications`.

Source réelle (crossref.py) :
  bucket   : raw-json
  path     : source=crossref/year=YYYY/month=MM/day=DD/crossref_*.json
  contenu  : liste JSON avec : source_system, source_url, extraction_timestamp,
             doi, title, authors (liste), publication_year, journal, abstract,
             content_hash.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transformations.spark.clean_data import add_record_id, normalize_text, drop_null_and_duplicates


def transform_publications(raw_df: DataFrame) -> DataFrame:
    df = raw_df.select(
        F.col("doi").alias("doi"),
        F.col("title").alias("title"),
        F.col("journal").alias("journal"),
        F.col("abstract").alias("abstract"),
        F.col("publication_year").cast("int").alias("publication_year"),
        F.col("authors").alias("authors"),
        F.col("source_url").alias("source_url"),
        F.col("source_system").alias("source_system"),
        F.col("content_hash").alias("content_hash"),
        F.col("extraction_timestamp").cast("timestamp").alias("crawl_timestamp"),
    )

    df = add_record_id(df, key_cols=["doi", "source_url"])
    df = normalize_text(df, source_col="title")

    df = (
        df.withColumn("business_timestamp", F.col("crawl_timestamp"))
        .withColumn("is_deleted", F.lit(False))
        .withColumn("language", F.lit("en"))
        .na.fill({"publication_year": 0, "journal": "unknown"})
    )

    df = drop_null_and_duplicates(df, not_null_col="title")
    return df