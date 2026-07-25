from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from src.transformations.transformers.base_transformer import (
    deduplicate_by,
    drop_nulls,
    normalize_string,
)
from src.transformations.utils.logger import get_logger
from src.transformations.utils.metadata import add_processing_timestamp
from src.transformations.utils.schema_validator import validate_schema

logger = get_logger(__name__)

DOCUMENTS_TARGET_SCHEMA = StructType([
    StructField("record_id", StringType(), True),
    StructField("document_name", StringType(), True),
    StructField("document_type", StringType(), True),
    StructField("category", StringType(), True),
    StructField("language", StringType(), True),
    StructField("storage_path", StringType(), True),
    StructField("file_size", LongType(), True),
    StructField("checksum", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("crawl_timestamp", TimestampType(), True),
    StructField("processing_timestamp", TimestampType(), True),
])

FIELD_MAPPINGS: Dict[str, str] = {
    "record_id": "record_id",
    "file_name": "document_name",
    "content_type": "document_type",
    "raw_storage_path": "storage_path",
    "file_size_bytes": "file_size",
    "content_hash": "checksum",
    "source_system": "source_system",
    "source_url": "source_url",
    "crawl_timestamp": "crawl_timestamp",
}


def _map_fields(df: DataFrame) -> DataFrame:
    selected_targets: Dict[str, list] = {}
    used_raw = set()
    for raw_col, target in FIELD_MAPPINGS.items():
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


def transform_documents_registry(df: DataFrame) -> DataFrame:
    if df.count() == 0:
        logger.warning("Empty input DataFrame, returning empty result")
        return df

    logger.info("Transforming documents registry")

    df = _map_fields(df)

    df = normalize_string(df, ["document_name", "document_type"])

    if "category" not in df.columns:
        df = df.withColumn("category", F.lit(None).cast(StringType()))

    if "language" not in df.columns:
        df = df.withColumn("language", F.lit(None).cast(StringType()))

    df = drop_nulls(df, subset=["document_name"])

    if "file_size" in df.columns:
        df = df.withColumn("file_size", F.col("file_size").cast(LongType()))

    if "source_system" not in df.columns:
        df = df.withColumn("source_system", F.lit("document_crawler"))

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
                F.lit("doc"),
                F.sha2(F.to_json(F.struct(F.col("*"))), 256).substr(1, 16),
            ),
        )

    df = add_processing_timestamp(df)
    df = deduplicate_by(df, keys=["record_id"])

    for col_name in DOCUMENTS_TARGET_SCHEMA.names:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))

    df = df.select(*DOCUMENTS_TARGET_SCHEMA.names)
    validate_schema(df, DOCUMENTS_TARGET_SCHEMA)
    logger.info("Documents registry transformation complete", extra={"records": df.count()})
    return df
