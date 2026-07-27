"""
Transformation USMS News (extraites pendant le crawl unique) -> schéma curated `university_news`.

Source réelle (usms_scraper.py -> save_consolidated_news()) :
  bucket   : raw-json
  path     : university_news/year=YYYY/month=MM/day=DD/university_news_*.json
  contenu  : UN objet JSON par fichier, clé "news_items" (liste imbriquée).
             Chaque entrée a : record_id, source_system, source_url,
             content_hash, crawl_timestamp, business_timestamp, is_deleted,
             language, normalized_text (vide) + title, content, category,
             image_url (URL d'origine), image_storage_path (chemin réel
             dans raw-images/MinIO), article_url, publish_date, institution.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transformations.spark.clean_data import normalize_text, drop_null_and_duplicates


def transform_news(raw_df: DataFrame) -> DataFrame:
    exploded = raw_df.select(F.explode("news_items").alias("n"))

    df = exploded.select(
        F.col("n.record_id").alias("record_id"),
        F.col("n.source_system").alias("source_system"),
        F.col("n.source_url").alias("source_url"),
        F.col("n.content_hash").alias("content_hash"),
        F.col("n.crawl_timestamp").cast("timestamp").alias("crawl_timestamp"),
        F.col("n.is_deleted").alias("is_deleted"),
        F.col("n.language").alias("language"),
        F.col("n.title").alias("title"),
        F.col("n.content").alias("content"),
        F.col("n.category").alias("category"),
        F.col("n.image_url").alias("image_url"),
        F.col("n.image_storage_path").alias("image_storage_path"),
        F.col("n.article_url").alias("article_url"),
        F.col("n.institution").alias("institution"),
        F.col("n.publish_date").cast("timestamp").alias("business_timestamp"),
    )

    df = df.withColumn(
        "business_timestamp",
        F.coalesce(F.col("business_timestamp"), F.col("crawl_timestamp")),
    )
    df = df.withColumn("publication_year", F.year(F.col("business_timestamp")))
    df = normalize_text(df, source_col="content")
    df = df.na.fill({
        "category": "general",
        "institution": "unknown",
        "image_url": "",
        "image_storage_path": "",
    })
    df = drop_null_and_duplicates(df, not_null_col="title")

    return df