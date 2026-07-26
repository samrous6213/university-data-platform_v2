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
    drop_nulls,
    fill_defaults,
    normalize_string,
)
from src.transformations.utils.logger import get_logger
from src.transformations.utils.metadata import add_processing_timestamp
from src.transformations.utils.schema_validator import validate_schema

logger = get_logger(__name__)

COURSE_TARGET_SCHEMA = StructType([
    StructField("record_id", StringType(), True),
    StructField("course_code", StringType(), True),
    StructField("course_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("credits", IntegerType(), True),
    StructField("semester", StringType(), True),
    StructField("level", StringType(), True),
    StructField("department", StringType(), True),
    StructField("faculty", StringType(), True),
    StructField("language", StringType(), True),
    StructField("instructor", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("crawl_timestamp", TimestampType(), True),
    StructField("processing_timestamp", TimestampType(), True),
])

POSSIBLE_ARRAY_FIELDS: List[str] = [
    "course_items",
    "courses",
    "news_items",
    "items",
    "results",
]

FIELD_MAPPINGS: Dict[str, Dict[str, str]] = {
    "course_scraper": {
        "code": "course_code",
        "course_id": "course_code",
        "name": "course_name",
        "course_name": "course_name",
        "title": "course_name",
        "description": "description",
        "credits": "credits",
        "semester": "semester",
        "level": "level",
        "department": "department",
        "faculty": "faculty",
        "language": "language",
        "instructor": "instructor",
        "instructor_name": "instructor",
        "url": "source_url",
        "source_url": "source_url",
    },
    "news_scraper": {
        "title": "course_name",
        "description": "description",
        "source": "faculty",
        "institution": "faculty",
        "url": "source_url",
        "source_url": "source_url",
    },
}
DEFAULT_FACULTY = "Général"

SOURCE_MAPPING = {
    "all_institutions_marrakech": "news_scraper",
    "ensa": "news_scraper",
}


def _map_to_course_schema(df: DataFrame, source_type: str) -> DataFrame:
    mapping = FIELD_MAPPINGS.get(source_type, {})
    if not mapping:
        return df

    selected_targets: Dict[str, List[str]] = {}
    all_exprs = []
    used_raw = set()
    for raw_col, target in mapping.items():
        if raw_col in df.columns:
            if target not in selected_targets:
                selected_targets[target] = []
            selected_targets[target].append(raw_col)
            used_raw.add(raw_col)

    for target, raw_cols in selected_targets.items():
        if len(raw_cols) == 1:
            all_exprs.append(F.col(raw_cols[0]).alias(target))
        else:
            all_exprs.append(F.coalesce(*[F.col(c) for c in raw_cols]).alias(target))

    for col in df.columns:
        if col not in used_raw and col not in selected_targets and not col.startswith("_"):
            all_exprs.append(F.col(col))

    return df.select(all_exprs)


def transform_course_catalog(df: DataFrame, source_name: str) -> DataFrame:
    if df.isEmpty():
        logger.warning("Empty input DataFrame, returning empty result")
        return df

    source_type = source_name
    mapping_key = SOURCE_MAPPING.get(source_name, source_name)
    logger.info(f"Transforming course data from source_type={source_type} (mapping_key={mapping_key})")

    df = _map_to_course_schema(df, mapping_key)

    if "course_name" not in df.columns and "title" in df.columns:
        df = df.withColumnRenamed("title", "course_name")

    df = normalize_string(
        df,
        ["course_name", "description", "course_code", "instructor", "department"],
    )

    df = drop_nulls(df, subset=["course_name"])
    df = fill_defaults(
        df,
        {
            "faculty": DEFAULT_FACULTY,
            "department": DEFAULT_FACULTY,
            "language": "fr",
            "credits": "0",
        },
    )

    if "credits" in df.columns:
        df = df.withColumn("credits", F.col("credits").cast(IntegerType()))
    else:
        df = df.withColumn("credits", F.lit(0).cast(IntegerType()))

    df = df.withColumn("source_system", F.lit(source_type))

    if "content_hash" not in df.columns or "record_id" not in df.columns:
        if "content_hash" not in df.columns:
            row_hash = F.sha2(F.to_json(F.struct(F.col("*"))), 256)
            df = df.withColumn("content_hash", row_hash)

        if "record_id" not in df.columns:
            if "id" in df.columns:
                df = df.withColumn(
                    "record_id",
                    F.concat_ws("_", F.col("source_system"), F.col("id")),
                )
            else:
                row_hash = F.sha2(F.to_json(F.struct(F.col("*"))), 256)
                df = df.withColumn(
                    "record_id",
                    F.concat_ws("_", F.col("source_system"), row_hash.substr(1, 16)),
                )

    if "crawl_timestamp" not in df.columns:
        df = df.withColumn("crawl_timestamp", F.current_timestamp())

    df = add_processing_timestamp(df)

    for col_name in COURSE_TARGET_SCHEMA.names:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))

    df = df.select(*COURSE_TARGET_SCHEMA.names)

    validate_schema(df, COURSE_TARGET_SCHEMA)
    logger.info(
        f"Course transformation complete",
        extra={"source": source_type},
    )

    return df
