#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

from pyspark.sql import SparkSession

HIVE_METASTORE_URIS = os.getenv("HIVE_METASTORE_URIS", "thrift://hive-metastore:9083")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://university-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

MVP_TABLES: List[Dict[str, str]] = [
    {"name": "faculty_profiles",        "path": "s3a://hudi-curated/faculty_profiles",        "partition_field": "source_system"},
    {"name": "course_catalog",          "path": "s3a://hudi-curated/course_catalog",          "partition_field": "source_system"},
    {"name": "university_news",         "path": "s3a://hudi-curated/university_news",         "partition_field": "source_system"},
    {"name": "research_publications",   "path": "s3a://hudi-curated/research_publications",   "partition_field": "source_system"},
    {"name": "documents_registry",      "path": "s3a://hudi-curated/documents_registry",      "partition_field": "source_system"},
]

HIVE_SYNC_OPTIONS: Dict[str, str] = {
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.partition_extractor_class": (
        "org.apache.hudi.hive.MultiPartKeysValueExtractor"
    ),
}


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("HiveSyncAndValidate")
        .master("local[*]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("hive.metastore.uris", HIVE_METASTORE_URIS)
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .enableHiveSupport()
        .getOrCreate()
    )


def sync_table(spark: SparkSession, table: Dict[str, str]) -> int:
    name = table["name"]
    path = table["path"]
    partition_field = table["partition_field"]
    print(f"\n{'='*60}")
    print(f"  Syncing table: {name}")
    print(f"{'='*60}")

    try:
        df = spark.read.format("hudi").load(path)
    except Exception as e:
        print(f"  ERROR: Could not read Hudi table {name}: {e}")
        return 0

    count = df.count()
    print(f"  Read {count} records from {path}")

    if count == 0:
        print(f"  WARNING: Table {name} is empty, skipping sync")
        return 0

    options = {
        "hoodie.table.name": name,
        "hoodie.datasource.write.recordkey.field": "record_id",
        "hoodie.datasource.write.precombine.field": "processing_timestamp",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        **HIVE_SYNC_OPTIONS,
        "hoodie.datasource.hive_sync.table": name,
    }
    if partition_field:
        options["hoodie.datasource.write.partitionpath.field"] = partition_field
        options["hoodie.datasource.write.hive_style_partitioning"] = "true"
        options["hoodie.datasource.hive_sync.partition_fields"] = partition_field
        options["hoodie.datasource.hive_sync.partition_extractor_class"] = (
            "org.apache.hudi.hive.MultiPartKeysValueExtractor"
        )

    try:
        df.write.format("hudi").mode("append").options(**options).save(path)
        print(f"  SUCCESS: Table {name} synced to Hive Metastore ({count} records)")
    except Exception as e:
        print(f"  ERROR: Failed to sync {name}: {e}")
        return 0

    return count


def run_validation(spark: SparkSession) -> None:
    print(f"\n{'='*60}")
    print("  VALIDATION QUERIES")
    print(f"{'='*60}")

    queries = [
        ("SHOW DATABASES", True),
        ("SHOW TABLES", True),
        ("DESCRIBE FORMATTED faculty_profiles", True),
        ("DESCRIBE FORMATTED course_catalog", True),
        ("DESCRIBE FORMATTED university_news", True),
        ("DESCRIBE FORMATTED research_publications", True),
        ("DESCRIBE FORMATTED documents_registry", True),
        ("SELECT COUNT(*) AS cnt FROM faculty_profiles", True),
        ("SELECT COUNT(*) AS cnt FROM course_catalog", True),
        ("SELECT COUNT(*) AS cnt FROM university_news", True),
        ("SELECT COUNT(*) AS cnt FROM research_publications", True),
        ("SELECT COUNT(*) AS cnt FROM documents_registry", True),
        ("SELECT * FROM faculty_profiles LIMIT 10", True),
        ("SELECT * FROM course_catalog LIMIT 10", True),
        ("SELECT * FROM university_news LIMIT 10", True),
        ("SELECT * FROM research_publications LIMIT 10", True),
        ("SELECT * FROM documents_registry LIMIT 10", True),
    ]

    all_passed = True
    for query, expect_rows in queries:
        print(f"\n  SQL: {query}")
        print(f"  {'-'*60}")
        try:
            result = spark.sql(query)
            result.show(truncate=False)
            row_count = result.count()
            if expect_rows and row_count == 0:
                print(f"  WARNING: Query returned 0 rows")
            print(f"  OK ({row_count} rows)")
        except Exception as e:
            print(f"  FAILED: {e}")
            all_passed = False

    print(f"\n{'='*60}")
    if all_passed:
        print("  ALL VALIDATION QUERIES PASSED")
    else:
        print("  SOME VALIDATION QUERIES FAILED")
    print(f"{'='*60}")


def main() -> int:
    print("=" * 60)
    print("  HUDI TABLE SYNC & HIVE VALIDATION")
    print("=" * 60)
    print(f"  Hive Metastore: {HIVE_METASTORE_URIS}")
    print(f"  MinIO Endpoint: {MINIO_ENDPOINT}")

    spark = create_spark()
    print("  SparkSession created with Hive support")

    total_records = 0
    for table in MVP_TABLES:
        count = sync_table(spark, table)
        total_records += count

    print(f"\n  Total records synced across all tables: {total_records}")

    if total_records > 0:
        run_validation(spark)
    else:
        print("  ERROR: No tables synced, skipping validation")

    spark.stop()
    print("\n  Hive sync and validation complete")
    return 0 if total_records > 0 else 1


if __name__ == "__main__":
    sys.exit(main())