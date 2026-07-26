from __future__ import annotations

import re
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.transformations.utils.logger import get_logger

logger = get_logger(__name__)


def build_s3a_path(bucket: str, prefix: str = "") -> str:
    clean_prefix = prefix.strip("/")
    if clean_prefix:
        return f"s3a://{bucket}/{clean_prefix}"
    return f"s3a://{bucket}"


def extract_source_name(prefix: str) -> str:
    match = re.search(r"source=([^/]+)", prefix)
    if match:
        return match.group(1)
    return prefix.strip("/").replace("/", "_")


def discover_source_prefixes(spark: SparkSession, bucket: str) -> List[str]:
    try:
        hadoop = spark._jvm.org.apache.hadoop
        path = hadoop.fs.Path(f"s3a://{bucket}/")
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        statuses = fs.listStatus(path)
        prefixes = []
        for s in statuses:
            name = s.getPath().getName()
            if s.isDirectory() and name.startswith("source="):
                prefixes.append(f"{name}/")
        prefixes.sort()
        logger.info(
            f"Discovered {len(prefixes)} source prefixes in s3a://{bucket}",
            extra={"prefixes": prefixes},
        )
        return prefixes
    except Exception as e:
        logger.warning(f"Could not discover sources in s3a://{bucket}: {e}")
        return []


def read_json(
    spark: SparkSession,
    bucket: str,
    prefix: str = "",
    schema: Optional[StructType] = None,
    recursive: bool = True,
    multi_line: bool = False,
) -> DataFrame:
    path = build_s3a_path(bucket, prefix)
    logger.info(f"Reading JSON from {path}")

    try:
        reader = spark.read.format("json")
        if schema:
            reader = reader.schema(schema)
        if multi_line:
            reader = reader.option("multiLine", "true")
        if recursive:
            reader = reader.option("recursiveFileLookup", "true")

        reader = (
            reader
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
        )

        df = reader.load(path)
        df.cache()
        df.count()

        if df.isEmpty():
            logger.warning(f"No data found at {path}")
        else:
            logger.info(
                f"Read data from {path}",
                extra={
                    "columns": df.columns,
                    "bucket": bucket,
                    "prefix": prefix,
                },
            )

        return df
    except Exception as e:
        logger.warning(f"Could not read from {path}: {e}")
        return spark.createDataFrame([], schema=StructType([]))


def read_raw_records(
    spark: SparkSession,
    bucket: str,
    source_prefixes: Optional[List[str]] = None,
    array_fields: Optional[List[str]] = None,
) -> DataFrame:
    if source_prefixes is None:
        source_prefixes = discover_source_prefixes(spark, bucket)
        if not source_prefixes:
            logger.warning(f"No source prefixes found in s3a://{bucket}")
            return spark.createDataFrame([], schema=StructType([]))

    if array_fields is None:
        array_fields = []

    unioned: Optional[DataFrame] = None
    matched_count = 0

    for prefix in source_prefixes:
        raw = read_json(spark, bucket, prefix=prefix)
        if raw.isEmpty():
            continue

        source_name = extract_source_name(prefix)
        raw_with_source = raw.withColumn("_source_prefix", F.lit(source_name))

        if not array_fields:
            flat = raw_with_source.select(
                "*",
                F.input_file_name().alias("_source_file"),
            )
            if unioned is None:
                unioned = flat
            else:
                unioned = unioned.unionByName(flat, allowMissingColumns=True)
            matched_count += 1
            continue

        for field in array_fields:
            if field in raw.columns:
                exploded = raw_with_source.selectExpr(
                    f"inline_outer({field})",
                    "input_file_name() as _source_file",
                    "_source_prefix",
                )
                if unioned is None:
                    unioned = exploded
                else:
                    unioned = unioned.unionByName(exploded, allowMissingColumns=True)
                matched_count += 1
                break

    if unioned is None:
        logger.warning("No records found from any source")
        return spark.createDataFrame([], schema=StructType([]))

    unioned.cache()
    logger.info(
        f"Union complete",
        extra={"sources": len(source_prefixes), "matched": matched_count},
    )
    return unioned
