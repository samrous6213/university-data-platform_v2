from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.transformations.transformers.base_transformer import (
    deduplicate_by,
    drop_nulls,
    fill_defaults,
    normalize_string,
)
from src.transformations.utils.logger import get_logger
from src.transformations.utils.metadata import add_processing_timestamp
from src.transformations.utils.schema_validator import validate_schema

logger = get_logger(__name__)

NEWS_TARGET_SCHEMA = StructType([
    StructField("record_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("content", StringType(), True),
    StructField("publication_date", StringType(), True),
    StructField("author", StringType(), True),
    StructField("faculty", StringType(), True),
    StructField("category", StringType(), True),
    StructField("language", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("crawl_timestamp", TimestampType(), True),
    StructField("processing_timestamp", TimestampType(), True),
])

FIELD_MAPPINGS: Dict[str, Dict[str, str]] = {
    "news_scraper": {
        "title": "title",
        "normalized_text": "content",
        "publication_date": "publication_date",
        "institution": "faculty",
        "category": "category",
        "language": "language",
        "source_system": "source_system",
        "source_url": "source_url",
        "content_hash": "content_hash",
        "record_id": "record_id",
        "crawl_timestamp": "crawl_timestamp",
    },
}


def _map_fields(df: DataFrame, source_type: str) -> DataFrame:
    mapping = FIELD_MAPPINGS.get(source_type, {})
    if not mapping:
        return df

    selected_targets: Dict[str, list] = {}
    used_raw = set()
    for raw_col, target in mapping.items():
        if raw_col in df.columns:
            if target not in selected_targets:
                selected_targets[target] = []
            selected_targets[target].append(raw_col)
            used_raw.add(raw_col)

    all_exprs = []
    for target, raw_cols in selected_targets.items():
        if len(raw_cols) == 1:
            all_exprs.append(F.col(raw_cols[0]).alias(target))
        else:
            all_exprs.append(F.coalesce(*[F.col(c) for c in raw_cols]).alias(target))

    for col in df.columns:
        if col not in used_raw and col not in selected_targets and not col.startswith("_"):
            all_exprs.append(F.col(col))

    return df.select(all_exprs)


def transform_university_news(df: DataFrame) -> DataFrame:
    if df.count() == 0:
        logger.warning("Empty input DataFrame, returning empty result")
        return df

    source_type = "news_scraper"
    logger.info(f"Transforming university news from source_type={source_type}")

    df = _map_fields(df, source_type)

    df = normalize_string(df, ["title", "content", "summary"])

    if "summary" not in df.columns and "content" in df.columns:
        df = df.withColumn("summary", F.substring(F.col("content"), 1, 200))

    df = drop_nulls(df, subset=["title"])

    if "record_id" not in df.columns:
        df = df.withColumn(
            "record_id",
            F.concat_ws(
                "_",
                F.lit("news"),
                F.sha2(F.to_json(F.struct(F.col("*"))), 256).substr(1, 16),
            ),
        )

    if "source_system" not in df.columns:
        df = df.withColumn("source_system", F.lit(source_type))

    if "content_hash" not in df.columns:
        df = df.withColumn(
            "content_hash",
            F.sha2(F.to_json(F.struct(F.col("*"))), 256),
        )

    if "crawl_timestamp" not in df.columns:
        df = df.withColumn("crawl_timestamp", F.current_timestamp())

    df = add_processing_timestamp(df)

    df = deduplicate_by(df, keys=["record_id"])

    for col_name in NEWS_TARGET_SCHEMA.names:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))

    df = df.select(*NEWS_TARGET_SCHEMA.names)
    validate_schema(df, NEWS_TARGET_SCHEMA)
    logger.info("University news transformation complete", extra={"records": df.count()})
    return df
