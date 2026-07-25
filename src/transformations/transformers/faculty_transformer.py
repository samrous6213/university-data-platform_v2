from __future__ import annotations

from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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

FACULTY_TARGET_SCHEMA = StructType([
    StructField("record_id", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("title", StringType(), True),
    StructField("department", StringType(), True),
    StructField("faculty", StringType(), True),
    StructField("university", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("office", StringType(), True),
    StructField("profile_url", StringType(), True),
    StructField("research_interests", StringType(), True),
    StructField("publications_count", IntegerType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("crawl_timestamp", TimestampType(), True),
    StructField("processing_timestamp", TimestampType(), True),
])

FIELD_MAPPINGS: Dict[str, Dict[str, str]] = {
    "openalex": {
        "display_name": "full_name",
        "last_name": "last_name",
        "first_name": "first_name",
        "email": "email",
        "department": "department",
        "institution": "university",
        "homepage_url": "profile_url",
        "research_interests": "research_interests",
        "works_count": "publications_count",
        "id": "source_url",
    },
    "faculty_scraper": {
        "full_name": "full_name",
        "first_name": "first_name",
        "last_name": "last_name",
        "title": "title",
        "department": "department",
        "institution": "faculty",
        "email": "email",
        "phone": "phone",
        "office": "office",
        "profile_url": "profile_url",
        "source_url": "source_url",
        "research_interests": "research_interests",
    },
    "faculty_web_scraper": {
        "full_name": "full_name",
        "first_name": "first_name",
        "last_name": "last_name",
        "title": "title",
        "department": "department",
        "institution": "faculty",
        "email": "email",
        "phone": "phone",
        "office": "office",
        "profile_url": "profile_url",
        "source_url": "source_url",
        "research_interests": "research_interests",
    },
}

DEFAULT_FACULTY = "Général"
DEFAULT_UNIVERSITY = "Université Cadi Ayyad"


def _detect_source_system(df: DataFrame) -> str:
    if "source_system" in df.columns:
        first = df.select(F.first("source_system", ignorenulls=True)).collect()[0][0]
        if first:
            return str(first)
    if "id" in df.columns:
        return "openalex"
    return "unknown"


def _map_fields(df: DataFrame, source_system: str) -> DataFrame:
    mapping = FIELD_MAPPINGS.get(source_system, {})
    if not mapping:
        logger.warning(
            f"No field mapping for source_system={source_system}, using passthrough"
        )
        return df

    selected_targets: set = set()
    all_exprs = []
    used_raw = set()
    for raw_col, target in mapping.items():
        if raw_col in df.columns:
            all_exprs.append(F.col(raw_col).alias(target))
            used_raw.add(raw_col)
            selected_targets.add(target)

    for col in df.columns:
        if col not in used_raw and col not in selected_targets and not col.startswith("_"):
            all_exprs.append(F.col(col))

    return df.select(all_exprs)


def _build_full_name(df: DataFrame) -> DataFrame:
    has_full = "full_name" in df.columns
    has_first = "first_name" in df.columns
    has_last = "last_name" in df.columns

    if not has_full and has_first and has_last:
        df = df.withColumn(
            "full_name",
            F.trim(F.concat_ws(" ", F.col("first_name"), F.col("last_name"))),
        )
    elif not has_full and has_last:
        df = df.withColumn("full_name", F.col("last_name"))
    elif not has_full and has_first:
        df = df.withColumn("full_name", F.col("first_name"))

    if has_first and not has_last:
        df = df.withColumn("last_name", F.lit(None).cast(StringType()))
    if has_last and not has_first:
        df = df.withColumn("first_name", F.lit(None).cast(StringType()))

    return df


def _extract_first_last(df: DataFrame) -> DataFrame:
    has_first = "first_name" in df.columns
    has_last = "last_name" in df.columns
    has_full = "full_name" in df.columns

    if has_full and (not has_first or not has_last):
        if not has_first:
            df = df.withColumn(
                "first_name",
                F.split(F.col("full_name"), r"\s+").getItem(0),
            )
        if not has_last:
            df = df.withColumn(
                "last_name",
                F.when(
                    F.length(F.col("full_name")) > F.length(F.col("first_name")),
                    F.trim(
                        F.expr(
                            "substring(full_name, length(first_name) + 1, "
                            "length(full_name))"
                        )
                    ),
                ).otherwise(F.lit(None)),
            )
    return df


def transform_faculty_profiles(df: DataFrame) -> DataFrame:
    if df.count() == 0:
        logger.warning("Empty input DataFrame, returning empty result")
        return df

    source_system = _detect_source_system(df)
    logger.info(f"Transforming faculty data from source_system={source_system}")

    df = _map_fields(df, source_system)
    df = _build_full_name(df)
    df = _extract_first_last(df)

    defaults = {
        "faculty": DEFAULT_FACULTY,
        "university": DEFAULT_UNIVERSITY,
        "department": DEFAULT_FACULTY,
    }
    for col_name, default_val in defaults.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(default_val))

    df = normalize_string(
        df,
        ["full_name", "first_name", "last_name", "email", "department", "title"],
    )

    df = drop_nulls(df, subset=["first_name", "last_name"])
    df = fill_defaults(df, defaults)

    if "publications_count" in df.columns:
        df = df.withColumn(
            "publications_count", F.col("publications_count").cast(IntegerType())
        )
    else:
        df = df.withColumn("publications_count", F.lit(0).cast(IntegerType()))

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
                F.col("source_system"),
                F.sha2(F.to_json(F.struct(F.col("*"))), 256).substr(1, 16),
            ),
        )

    df = add_processing_timestamp(df)
    df = deduplicate_by(df, keys=["record_id"])

    for col_name in FACULTY_TARGET_SCHEMA.names:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))

    df = df.select(*FACULTY_TARGET_SCHEMA.names)

    validate_schema(df, FACULTY_TARGET_SCHEMA)
    logger.info(
        f"Faculty transformation complete",
        extra={"records": df.count(), "source": source_system},
    )

    return df
