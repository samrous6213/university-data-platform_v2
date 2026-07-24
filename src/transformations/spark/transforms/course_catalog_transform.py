"""
Transformation : pages web classifiees 'course_catalog' -> table curated course_catalog.

Entree attendue (depuis readers.json_reader.read_web_crawler_json(spark, "course_catalog")) :
    source_url, extraction_timestamp, http_status, content_checksum,
    connector_version, school_id, school_name, entity_type, extracted_text,
    html_object_path, json_object_path
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import array, coalesce, col, lit, lower, regexp_extract, to_timestamp, when

from src.transformations.spark.transforms.text_cleaning import (
    add_is_deleted_flag,
    add_language,
    add_record_id,
    normalize_text_column,
)

logger = logging.getLogger(__name__)

# Memes mots-cles que UNIVERSAL_KEYWORDS["course_catalog"] dans generic_crawler.py,
# reutilises ici pour deriver program_level a partir du texte de la page.
_LEVEL_PATTERN = r"(licence|master|ingenieur|doctorat)"
_PROGRAM_NAME_PATTERN = r"((?:Licence|Master|Ingenieur|Doctorat)[^\.\n]{0,80})"


def transform_course_catalog(df_web: DataFrame) -> DataFrame:
    df = df_web

    if "raw_object_path" not in df.columns:
        df = df.withColumn("raw_object_path", coalesce(col("html_object_path"), col("json_object_path")))

    df = normalize_text_column(df, source_col="extracted_text", target_col="normalized_text")
    df = add_record_id(df, url_col="source_url", hash_col="content_hash")
    df = add_language(df, text_col="normalized_text")
    df = add_is_deleted_flag(df)

    df = df.withColumn(
        "program_level",
        lower(regexp_extract(col("extracted_text"), _LEVEL_PATTERN, 1)),
    )
    df = df.withColumn(
        "program_level",
        when(col("program_level") == "", lit(None)).otherwise(col("program_level")),
    )
    df = df.withColumn(
        "program_name",
        regexp_extract(col("extracted_text"), _PROGRAM_NAME_PATTERN, 1),
    )
    df = df.withColumn(
        "program_name",
        when(col("program_name") == "", lit(None)).otherwise(col("program_name")),
    )
    df = df.withColumn("department", lit(None).cast("string"))  # a affiner : mapping par gabarit de page
    df = df.withColumn("keywords_matched", array().cast("array<string>"))  # a affiner si besoin de detail
    df = df.withColumn("catalog_url", col("source_url"))

    df = df.withColumn("crawl_timestamp", to_timestamp(col("extraction_timestamp")))
    df = df.withColumn("business_timestamp", col("crawl_timestamp"))
    df = df.withColumn("source_system", lit("web_crawler"))
    df = df.withColumnRenamed("content_checksum", "content_hash")

    final_columns = [
        "record_id", "source_system", "source_url", "raw_object_path", "content_hash",
        "crawl_timestamp", "business_timestamp", "is_deleted", "language",
        "school_id", "school_name", "program_name", "program_level", "department",
        "keywords_matched", "catalog_url", "normalized_text",
    ]
    df = df.select(*final_columns)

    logger.info("Transformation course_catalog terminee : %s lignes", df.count())
    return df