from __future__ import annotations

from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

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

PUBLICATIONS_TARGET_SCHEMA = StructType([
    StructField("record_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("abstract", StringType(), True),
    StructField("authors", StringType(), True),
    StructField("affiliations", StringType(), True),
    StructField("publication_year", IntegerType(), True),
    StructField("doi", StringType(), True),
    StructField("journal", StringType(), True),
    StructField("keywords", StringType(), True),
    StructField("language", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("crawl_timestamp", TimestampType(), True),
    StructField("processing_timestamp", TimestampType(), True),
])

FIELD_MAPPINGS: Dict[str, Dict[str, str]] = {
    "openalex": {
        "display_name": "title",
        "id": "source_url",
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


def transform_research_publications(df: DataFrame) -> DataFrame:
    if df.count() == 0:
        logger.warning("Empty input DataFrame, returning empty result")
        return df

    source_system = "openalex" if "id" in df.columns else "unknown"
    logger.info(f"Transforming research publications from source={source_system}")

    df = _map_fields(df, source_system)

    df = normalize_string(df, ["title", "abstract", "authors", "keywords"])

    df = drop_nulls(df, subset=["title"])

    if "publication_year" in df.columns:
        df = df.withColumn("publication_year", F.col("publication_year").cast(IntegerType()))

    if "source_system" not in df.columns:
        df = df.withColumn("source_system", F.lit(source_system))

    if "content_hash" not in df.columns:
        df = df.withColumn(
            "content_hash",
            F.sha2(F.to_json(F.struct(F.col("*"))), 256),
        )

    if "crawl_timestamp" not in df.columns:
        df = df.withColumn("crawl_timestamp", F.current_timestamp())

    if "record_id" not in df.columns:
        df = df.withColumn(
            "record_id",
            F.concat_ws(
                "_",
                F.lit("pub"),
                F.sha2(F.to_json(F.struct(F.col("*"))), 256).substr(1, 16),
            ),
        )

    df = add_processing_timestamp(df)
    df = deduplicate_by(df, keys=["record_id"])

    for col_name in PUBLICATIONS_TARGET_SCHEMA.names:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))

    df = df.select(*PUBLICATIONS_TARGET_SCHEMA.names)
    validate_schema(df, PUBLICATIONS_TARGET_SCHEMA)
    logger.info("Research publications transformation complete", extra={"records": df.count()})
    return df
